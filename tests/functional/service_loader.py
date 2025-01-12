import asyncio
import json
from multiprocessing import Process
from typing import List, Optional

import pytest_asyncio
import uvicorn

from esds.collector.collector_manager import CollectorManager
from esds.collector.config import settings
from esds.collector.main import load_collectors, wait_for_indexer
from esds.indexer import app as indexer_app
from esds.retriever import app as retriever_app

INDEXER_PORT = 8000
RETRIEVER_PORT = 8001


@pytest_asyncio.fixture(loop_scope="class", scope="class")
async def indexer_service():
    async for _ in _start_uvicorn(indexer_app, port=INDEXER_PORT):
        yield


@pytest_asyncio.fixture(loop_scope="class", scope="class")
async def retriever_service():
    async for _ in _start_uvicorn(retriever_app, port=RETRIEVER_PORT):
        yield


async def _start_uvicorn(app, port):
    """Start service as test fixture and tear down after test"""

    def run_server():
        uvicorn.run(app, port=port)

    # Start Uvicorn in a separate process
    process = Process(target=run_server, daemon=True)
    process.start()

    # Wait for the server to start
    await asyncio.sleep(1)

    # Yield control back to the test
    yield

    # Stop the server after the test
    process.terminate()
    process.join()


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def collector_service():
    """Start server as test fixture and tear down after test"""
    await wait_for_indexer()
    collectors = await load_collectors()
    print("Starting collectors...")

    cm = await CollectorManager.create(collectors)
    await cm.start_all_collectors()
    await cm.wait_for_startup()
    yield
    await cm.close()
    await cm.join()


class ConfigurableCollectorService:
    def generate_collector_definitions_file(
        self, n_pvs: int, pv_len: int, n_collectors: int
    ):
        collectors = [
            {
                "name": "collector_" + str(i),
                "parent_path": "/",
                "event_code": 1,
                "pvs": [
                    (
                        f"SDS:TEST:PV_{pv_len}_{j}"
                        if n_pvs > 1
                        else f"SDS:TEST:PV_{pv_len}"
                    )
                    for j in range(n_pvs)
                ],
            }
            for i in range(n_collectors)
        ]
        with open("collector_perf_config.json", "w") as config:
            json.dump(collectors, config, indent=4)
            settings.collector_definitions = config.name

    async def start(self):
        await wait_for_indexer()
        collectors = await load_collectors()
        print("Starting collectors...")

        self.cm = await CollectorManager.create(collectors)
        await self.cm.start_all_collectors()
        await self.cm.wait_for_startup()

    async def stop(self):
        await self.cm.close()
        await self.cm.join()


@pytest_asyncio.fixture()
async def configurable_collector_service():
    """Start server as test fixture and tear down after test"""

    collector_service = ConfigurableCollectorService()
    yield collector_service
    await collector_service.stop()
