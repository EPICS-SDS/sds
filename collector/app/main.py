from typing import List

import logging
import asyncio
import aiohttp
from pydantic import parse_file_as

from app.collector_manager import CollectorManager
from app.collector import Collector, CollectorSchema
from app.config import settings

from p4p import set_debug

set_debug(logging.WARNING)


async def load_collectors():
    path = settings.collector_definitions
    print(f"Loading collector definitions from {path}")
    async with aiohttp.ClientSession(
            json_serialize=CollectorSchema.json) as session:
        collectors = []
        for collector in parse_file_as(List[CollectorSchema], path):
            print(f"Collector '{collector.name}' loaded from file")
            async with session.post(
                settings.indexer_url + "/collectors",
                json=collector,
            ) as response:
                response.raise_for_status()
                if response.status == 202:
                    print(f"Collector '{collector.name}' created in DB")
                else:
                    print(f"Collector '{collector.name}' already in DB")
                obj = await response.json()
                collectors.append(Collector.parse_obj(obj))
        return collectors


async def main():
    print("SDS Collector service\n")

    collectors = await load_collectors()
    print("Starting collectors...")

    async with CollectorManager(collectors) as cm:
        await cm.join()


if __name__ == "__main__":
    asyncio.run(main())
