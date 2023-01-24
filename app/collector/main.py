import asyncio
import logging
from typing import List

import aiohttp
from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientOSError,
    ClientResponseError,
)
from collector.api import start_api
from collector.collector import Collector
from collector.collector_manager import CollectorManager
from collector.config import settings
from common.schemas import CollectorBase
from p4p import set_debug
from pydantic import parse_file_as

set_debug(logging.WARNING)


async def load_collectors():
    indexer_timeout = settings.indexer_timeout_min

    path = settings.collector_definitions
    print(f"Loading collector definitions from {path}")

    async with aiohttp.ClientSession(json_serialize=CollectorBase.json) as session:
        collectors = []
        for collector in parse_file_as(List[CollectorBase], path):
            print(f"Collector '{collector.name}' loaded from file")
            indexer_connected = False
            while not indexer_connected:
                try:
                    async with session.post(
                        settings.indexer_url + "/collectors",
                        json=collector,
                    ) as response:
                        response.raise_for_status()
                        if response.status == 201:
                            print(f"Collector '{collector.name}' created in DB")
                        elif response.status == 200:
                            print(f"Collector '{collector.name}' already in DB")
                        indexer_connected = True
                        obj = await response.json()
                        collectors.append(Collector.parse_obj(obj))
                except (
                    ClientConnectorError,
                    ClientResponseError,
                    ConnectionRefusedError,
                    ClientOSError,
                ):
                    if settings.wait_for_indexer:
                        print(
                            f"Could not connect to the indexer service {settings.indexer_url}. Retrying in {indexer_timeout} s."
                        )
                        await asyncio.sleep(indexer_timeout)
                        # doubling timeout for indexer until max timeout is reached
                        indexer_timeout = min(
                            indexer_timeout * 2, settings.indexer_timeout_max
                        )
                    else:
                        raise
        return collectors


async def main():
    print("SDS Collector service\n")
    collectors = await load_collectors()

    if settings.collector_api_enabled:
        serve_task = start_api()

    print("Starting collectors...")
    async with CollectorManager(collectors) as cm:
        await cm.join()

    if settings.collector_api_enabled:
        await serve_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt):
        pass
