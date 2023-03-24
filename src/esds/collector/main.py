import asyncio
import logging
from typing import List, Optional

import aiohttp
from aiohttp.client_exceptions import ClientError
from p4p import set_debug
from pydantic import parse_file_as
from esds.collector.api import start_api
from esds.collector.collector_manager import CollectorManager
from esds.collector.config import settings
from esds.common.files import CollectorDefinition

set_debug(logging.WARNING)


async def load_collectors() -> Optional[List[CollectorDefinition]]:
    path = settings.collector_definitions
    logging.info(f"Loading collector definitions from {path}")

    collectors = parse_file_as(Optional[List[CollectorDefinition]], path)
    return collectors


async def wait_for_indexer():
    if settings.wait_for_indexer:
        indexer_timeout = settings.indexer_timeout_min
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(
                        settings.indexer_url + "/health"
                    ) as response:
                        response.raise_for_status()
                        if response.status == 200:
                            logging.info("Indexer ready")
                            return
                except (
                    ClientError,
                    OSError,
                ):
                    pass

                logging.warning(
                    f"Could not connect to the indexer service {settings.indexer_url}. Retrying in {indexer_timeout} s."
                )
                await asyncio.sleep(indexer_timeout)
                # doubling timeout for indexer until max timeout is reached
                indexer_timeout = min(indexer_timeout * 2, settings.indexer_timeout_max)


async def main():
    await wait_for_indexer()

    logging.info("SDS Collector service\n")
    collectors = await load_collectors()

    if settings.collector_api_enabled:
        serve_task = start_api()

    logging.info("Starting collectors...")
    async with await CollectorManager.create(collectors) as cm:
        await cm.join()

    if settings.collector_api_enabled:
        await serve_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt):
        pass