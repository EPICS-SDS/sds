from typing import List

import logging
import asyncio
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ClientResponseError
from pydantic import parse_file_as

from collector.collector_manager import CollectorManager
from collector.collector import Collector, CollectorSchema
from collector.config import settings

from p4p import set_debug

set_debug(logging.WARNING)


async def load_collectors():
    path = settings.collector_definitions
    print(f"Loading collector definitions from {path}")
    async with aiohttp.ClientSession(json_serialize=CollectorSchema.json) as session:
        collectors = []
        for collector in parse_file_as(List[CollectorSchema], path):
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
                except (ClientConnectorError, ClientResponseError):
                    if settings.wait_for_indexer:
                        print(
                            f"Could not connect to the indexer service {settings.indexer_url}. Retrying in {settings.indexer_timeout} s."
                        )
                        await asyncio.sleep(settings.indexer_timeout)
                    else:
                        raise
        return collectors


async def main():
    print("SDS Collector service\n")

    collectors = await load_collectors()
    print("Starting collectors...")

    async with CollectorManager(collectors) as cm:
        await cm.join()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt):
        pass
