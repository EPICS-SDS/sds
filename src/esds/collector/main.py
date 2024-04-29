import argparse
import asyncio
import json
import logging
from typing import List, Optional
from urllib.parse import urljoin

import aiohttp
from aiohttp.client_exceptions import ClientError
from p4p import set_debug
from pydantic import TypeAdapter

from esds.collector.api import start_api
from esds.collector.collector_manager import CollectorManager
from esds.collector.config import settings
from esds.common.files import CollectorList

ch = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
ch.setFormatter(formatter)
ch.setLevel(settings.log_level)
logging.getLogger().addHandler(ch)
logging.getLogger().setLevel(settings.log_level)


logger = logging.getLogger(__name__)

set_debug(logging.WARNING)


async def load_collectors() -> Optional[CollectorList]:
    path = settings.collector_definitions
    logger.info(f"Loading collector definitions from {path}")

    with open(path, "r") as settings_file:
        collectors = TypeAdapter(Optional[CollectorList]).validate_python(
            json.load(settings_file)
        )
    return collectors


async def wait_for_indexer():
    if settings.wait_for_indexer:
        indexer_timeout = settings.indexer_timeout_min
        async with aiohttp.ClientSession(conn_timeout=5, read_timeout=5) as session:
            while True:
                try:
                    logger.info(f'url = {urljoin(str(settings.indexer_url),"/health")}')
                    async with session.get(
                        urljoin(str(settings.indexer_url), "/health")
                    ) as response:
                        response.raise_for_status()
                        if response.status == 200:
                            logger.info("Indexer ready")
                            return
                except (
                    ClientError,
                    OSError,
                ) as e:
                    logger.warning(e)
                    pass

                logger.warning(
                    f"Could not connect to the indexer service {settings.indexer_url}. Retrying in {indexer_timeout} s."
                )
                await asyncio.sleep(indexer_timeout)
                # doubling timeout for indexer until max timeout is reached
                indexer_timeout = min(indexer_timeout * 2, settings.indexer_timeout_max)


async def main(root_path: str, reload: bool):
    await wait_for_indexer()

    logger.info("SDS Collector service\n")
    collectors = await load_collectors()

    if settings.collector_api_enabled:
        serve_task = start_api(root_path=root_path, reload=reload)

    logger.info("Starting collectors...")
    async with await CollectorManager.create(collectors) as cm:
        await cm.join()

    if settings.collector_api_enabled:
        await serve_task


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collector service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--root-path", help="root-path for FastAPI", default="")
    parser.add_argument(
        "--reload", help="reload option for FastAPI", action="store_true", default=False
    )

    args = parser.parse_args()

    try:
        asyncio.run(main(root_path=args.root_path, reload=args.reload))
    except KeyboardInterrupt:
        pass
