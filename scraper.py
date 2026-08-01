import asyncio
import datetime
import ipaddress
import json
import logging

import aiofiles
import httpx
import tldextract
from tqdm import tqdm

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
    return datetime.datetime.now(datetime.UTC).strftime("%d_%b_%Y_%H_%M_%S-UTC")


semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10


async def fetch_page(client, url):
    async with semaphore:
        try:
            response = await client.get(url, timeout=300)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
        return None


async def fetch_models(client: httpx.AsyncClient, endpoint: str) -> list:
    """Fetch all models from a paginated endpoint.

    Args:
        client: AsyncClient instance for making requests
        endpoint: API endpoint URL

    Returns:
        list: All models from all pages
    """
    # Fetch first page to get total page count
    response = await client.get(endpoint + "&page=0", timeout=300)
    page_data = json.loads(response.content)

    if not isinstance(page_count := page_data.get("pageCount"), int):
        return page_data.get("models", [])

    models = page_data.get("models", [])

    # Concurrently fetch remaining pages
    if page_count > 1:
        tasks = [
            fetch_page(client, endpoint + f"&page={page}")
            for page in range(1, page_count + 1)
        ]
        for page_data in await asyncio.gather(*tasks):
            if page_data is None:
                continue

            if isinstance(page_models := page_data.get("models"), list):
                models.extend(page_models)

    return models


async def extract_urls():
    endpoint = "https://siberguvenlik.gov.tr/api/address/index"
    endpoint_for_domain = endpoint + "?type=domain&per-page=9999"
    endpoint_for_ip = endpoint + "?type=ip&per-page=9999"
    endpoint_for_ip6 = endpoint + "?type=ip6&per-page=9999"

    non_ips: set[str] = set()
    ips: set[str] = set()
    fqdns: set[str] = set()
    registered_domains: set[str] = set()

    async with httpx.AsyncClient(headers=default_headers) as client:
        models_for_domain = await fetch_models(client, endpoint_for_domain)
        models_for_ip = await fetch_models(client, endpoint_for_ip)
        models_for_ip6 = await fetch_models(client, endpoint_for_ip6)

    # Process domain models - extract URLs, FQDNs, and registered domains
    for model in tqdm(models_for_domain, desc="Extracting URLs"):
        url = model.get("url", "").strip()
        if not url:
            continue
        res = tldextract.extract(url)
        non_ips.add(url)
        fqdns.add(res.fqdn)
        registered_domains.add(res.top_domain_under_public_suffix)

    # Process IP models - only add to ips set
    for model in tqdm(models_for_ip, desc="Extracting IPs"):
        url = model.get("url", "").strip()
        if not url:
            continue
        if url:
            ips.add(url)

    # Process IPv6 models - only add to ips set
    for model in tqdm(models_for_ip6, desc="Extracting IPv6s"):
        url = model.get("url", "").strip()
        if not url:
            continue
        if url:
            ips.add(url)

    if not non_ips and not ips:
        logger.error("No URLs found.")
        return

    non_ips_timestamp: str = current_datetime_str()
    non_ips_filename = "urls.txt"
    async with aiofiles.open(non_ips_filename, "w") as f:
        await f.write("\n".join(sorted(non_ips)))
        logger.info(
            "%d URLs written to %s at %s",
            len(non_ips),
            non_ips_filename,
            non_ips_timestamp,
        )

    ips_timestamp: str = current_datetime_str()
    ips_filename = "ips.txt"
    async with aiofiles.open(ips_filename, "w") as f:
        await f.write(
            "\n".join(
                sorted(
                    ips,
                    key=lambda ip: (
                        (addr := ipaddress.ip_address(ip)).version
                        and (addr.version, int(addr))
                    ),
                )
            )
        )
        logger.info(
            "%d IPs written to %s at %s",
            len(ips),
            ips_filename,
            ips_timestamp,
        )

    fqdns_timestamp: str = current_datetime_str()
    fqdns_filename = "urls_pihole.txt"
    async with aiofiles.open(fqdns_filename, "w") as f:
        await f.writelines("\n".join(sorted(fqdns)))
        logger.info(
            "%d FQDNs written to %s at %s",
            len(fqdns),
            fqdns_filename,
            fqdns_timestamp,
        )

    registered_domains_timestamp: str = current_datetime_str()
    registered_domains_filename = "urls_UBL.txt"
    async with aiofiles.open(registered_domains_filename, "w") as f:
        await f.writelines(
            "\n".join(f"*://*.{r}/*" for r in sorted(registered_domains))
        )
        logger.info(
            "%d Registered Domains written to %s at %s",
            len(registered_domains),
            registered_domains_filename,
            registered_domains_timestamp,
        )


if __name__ == "__main__":
    asyncio.run(extract_urls())
