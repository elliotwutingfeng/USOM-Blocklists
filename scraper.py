import asyncio
import ipaddress
import logging
import socket
from datetime import datetime

import aiohttp
import tldextract

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(message)s")

default_headers: dict = {
    "Connection": "keep-alive",
    "Accept": "*/*",
}


def current_datetime_str() -> str:
    """Current time's datetime string in UTC

    Returns:
        str: Timestamp in strftime format "%d_%b_%Y_%H_%M_%S-UTC".
    """
    return datetime.utcnow().strftime("%d_%b_%Y_%H_%M_%S-UTC")


async def backoff_delay_async(
    backoff_factor: float, number_of_retries_made: int
) -> None:
    """Asynchronous time delay that exponentially increases with `number_of_retries_made`

    Args:
        backoff_factor (float): Backoff delay multiplier
        number_of_retries_made (int): More retries made -> Longer backoff delay
    """
    await asyncio.sleep(backoff_factor * (2 ** (number_of_retries_made - 1)))


async def get_async(
    endpoints: list[str], max_concurrent_requests: int = 5, headers: dict | None = None
) -> dict[str, bytes]:
    """Given a list of HTTP endpoints, make HTTP GET requests asynchronously

    Args:
        endpoints (list[str]): List of HTTP GET request endpoints
        max_concurrent_requests (int, optional): Maximum number of concurrent async HTTP requests.
        Defaults to 5.
        headers (dict, optional): HTTP Headers to send with every request. Defaults to None.

    Returns:
        dict[str,bytes]: Mapping of HTTP GET request endpoint to its HTTP response content. If
        the GET request failed, its HTTP response content will be `b"{}"`
    """
    if headers is None:
        headers = default_headers

    async def gather_with_concurrency(
        max_concurrent_requests: int, *tasks
    ) -> dict[str, bytes]:
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async def sem_task(task):
            async with semaphore:
                await asyncio.sleep(0.5)
                return await task

        tasklist = [sem_task(task) for task in tasks]
        return dict([await f for f in asyncio.as_completed(tasklist)])

    async def get(url, session):
        max_retries: int = 8
        errors: list[str] = []
        for number_of_retries_made in range(max_retries):
            try:
                async with session.get(url, headers=headers) as response:
                    return (url, await response.read())
            except Exception as error:
                errors.append(repr(error))
                logger.warning(
                    "%s | Attempt %d failed", error, number_of_retries_made + 1
                )
                if (
                    number_of_retries_made != max_retries - 1
                ):  # No delay if final attempt fails
                    await backoff_delay_async(1, number_of_retries_made)
        logger.error("URL: %s GET request failed! Errors: %s", url, errors)
        return (url, b"{}")  # Allow json.loads to parse body if request fails

    # GET request timeout of 15 seconds
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=0, ttl_dns_cache=15),
        raise_for_status=True,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as session:
        # Only one instance of any duplicate endpoint will be used
        return await gather_with_concurrency(
            max_concurrent_requests, *[get(url, session) for url in set(endpoints)]
        )


async def extract_urls():
    endpoint = "https://www.usom.gov.tr/url-list.txt"

    data = (await get_async([endpoint]))[endpoint]
    non_ips: set[str] = set()
    ips: set[str] = set()
    fqdns: set[str] = set()
    registered_domains: set[str] = set()
    if data != b"{}":
        urls = data.decode().split("\n")
        for url in urls:
            url = url.strip()
            res = tldextract.extract(url)
            registered_domain, domain, fqdn = (
                res.registered_domain,
                res.domain,
                res.fqdn,
            )
            if domain and not fqdn:
                # Possible IPv4 Address
                try:
                    socket.inet_pton(socket.AF_INET, domain)
                    ips.add(domain)
                except socket.error:
                    if url:
                        non_ips.add(url)
            elif fqdn:
                non_ips.add(url)
                fqdns.add(fqdn)
                registered_domains.add(registered_domain)
    if not non_ips and not ips:
        logger.error("No URLs found.")
    else:
        non_ips_timestamp: str = current_datetime_str()
        non_ips_filename = "urls.txt"
        with open(non_ips_filename, "w") as f:
            f.write("\n".join(sorted(non_ips)))
            logger.info(
                "%d URLs written to %s at %s",
                len(non_ips),
                non_ips_filename,
                non_ips_timestamp,
            )

        ips_timestamp: str = current_datetime_str()
        ips_filename = "ips.txt"
        with open(ips_filename, "w") as f:
            f.write("\n".join(sorted(ips, key=ipaddress.IPv4Address)))
            logger.info(
                "%d IPs written to %s at %s",
                len(ips),
                ips_filename,
                ips_timestamp,
            )

        fqdns_timestamp: str = current_datetime_str()
        fqdns_filename = "urls_pihole.txt"
        with open(fqdns_filename, "w") as f:
            f.writelines("\n".join(sorted(fqdns)))
            logger.info(
                "%d FQDNs written to %s at %s",
                len(fqdns),
                fqdns_filename,
                fqdns_timestamp,
            )

        registered_domains_timestamp: str = current_datetime_str()
        registered_domains_filename = "urls_UBL.txt"
        with open(registered_domains_filename, "w") as f:
            f.writelines("\n".join(f"*://*.{r}/*" for r in sorted(registered_domains)))
            logger.info(
                "%d Registered Domains written to %s at %s",
                len(registered_domains),
                registered_domains_filename,
                registered_domains_timestamp,
            )


if __name__ == "__main__":
    asyncio.run(extract_urls())
