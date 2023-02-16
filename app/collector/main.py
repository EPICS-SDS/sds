import asyncio
import logging
import os
from typing import List

import aiohttp
from aiohttp.client_exceptions import ClientError
from collector.api import start_api
from collector.collector import Collector
from collector.collector_manager import CollectorManager
from collector.config import settings
from common.schemas import CollectorBase
from p4p import set_debug
from pydantic import parse_file_as

set_debug(logging.WARNING)


async def load_collectors():
    path = settings.collector_definitions
    print(f"Loading collector definitions from {path}")

    if not os.access(path, os.W_OK):
        print(
            "Collector definition file not writable. Any configuration change won't be saved."
        )

    collectors = parse_file_as(List[CollectorBase], path)
    return collectors


async def wait_for_indexer():
    if settings.wait_for_indexer:
        indexer_timeout = settings.indexer_timeout_min
        async with aiohttp.ClientSession(json_serialize=CollectorBase.json) as session:
            while True:
                try:
                    async with session.get(
                        settings.indexer_url + "/health"
                    ) as response:
                        response.raise_for_status()
                        if response.status == 200:
                            print("Indexer ready")
                            return
                except (
                    ClientError,
                    OSError,
                ):
                    pass

                print(
                    f"Could not connect to the indexer service {settings.indexer_url}. Retrying in {indexer_timeout} s."
                )
                await asyncio.sleep(indexer_timeout)
                # doubling timeout for indexer until max timeout is reached
                indexer_timeout = min(indexer_timeout * 2, settings.indexer_timeout_max)


async def main():
    await wait_for_indexer()

    print("SDS Collector service\n")
    collectors = await load_collectors()

    if settings.collector_api_enabled:
        serve_task = start_api()

    print("Starting collectors...")
    async with await CollectorManager.create(collectors) as cm:
        await cm.join()

    if settings.collector_api_enabled:
        await serve_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt):
        pass
