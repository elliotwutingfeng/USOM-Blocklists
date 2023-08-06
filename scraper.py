import asyncio
import ipaddress
import logging
import socket

import aiohttp
import tldextract

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(message)s")

default_headers: dict = {
    "Connection": "keep-alive",
    "Accept": "*/*",
}


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
    endpoints: list[str], max_concurrent_requests: int = 5, headers: dict = None
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
    urls: set[str] = set()
    ips: set[str] = set()
    if data != b"{}":
        entries = data.decode().split("\n")
        for entry in entries:
            entry = entry.strip()
            res = tldextract.extract(entry)
            domain, fqdn = res.domain, res.fqdn
            if domain and not fqdn:
                # Possible IPv4 Address
                try:
                    socket.inet_pton(socket.AF_INET, domain)
                    ips.add(domain)
                except socket.error:
                    if entry:
                        urls.add(entry)
            elif entry:
                urls.add(entry)
    if not urls and not ips:
        logger.error("No URLs found.")
    else:
        with open("urls.txt", "w") as f:
            f.write("\n".join(sorted(urls)))
        with open("ips.txt", "w") as f:
            f.write("\n".join(sorted(ips, key=ipaddress.IPv4Address)))


if __name__ == "__main__":
    asyncio.run(extract_urls())
