import asyncio
import json
from typing import List, Optional

import pytest_asyncio
import uvicorn
from collector.collector_manager import CollectorManager
from collector.main import load_collectors, wait_for_indexer
from collector.config import settings
from indexer import app as indexer_app
from retriever import app as retriever_app

INDEXER_PORT = 8000
RETRIEVER_PORT = 8001


class UvicornTestServer(uvicorn.Server):
    """Uvicorn test server

    Usage:
        @pytest.fixture
        server = UvicornTestServer()
        await server.up()
        yield
        await server.down()
    """

    def __init__(self, app, host="0.0.0.0", port=8000):
        """Create a Uvicorn test server

        Args:
            app (FastAPI, optional): the FastAPI app. Defaults to main.app.
            host (str, optional): the host ip. Defaults to '127.0.0.1'.
            port (int, optional): the port. Defaults to PORT.
        """
        self._startup_done = asyncio.Event()
        super().__init__(
            config=uvicorn.Config(
                app,
                host=host,
                port=port,
            )
        )

    async def startup(self, sockets: Optional[List] = None) -> None:
        """Override uvicorn startup"""
        await super().startup(sockets=sockets)
        self.config.setup_event_loop()
        self._startup_done.set()

    async def up(self) -> None:
        """Start up server asynchronously"""
        self._serve_task = asyncio.create_task(self.serve())
        await self._startup_done.wait()

    async def down(self) -> None:
        """Shut down server asynchronously"""
        self.should_exit = True
        await self._serve_task


@pytest_asyncio.fixture()
async def indexer_service():
    """Start indexer service as test fixture and tear down after test"""
    server = UvicornTestServer(indexer_app, port=INDEXER_PORT)
    await server.up()
    yield
    await server.down()


@pytest_asyncio.fixture()
async def retriever_service():
    """Start retriever service as test fixture and tear down after test"""
    server = UvicornTestServer(retriever_app, port=RETRIEVER_PORT)
    await server.up()
    yield
    await server.down()


@pytest_asyncio.fixture()
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
                "event_name": "data-on-demand",
                "event_code": 1,
                "pvs": [
                    f"SDS:TEST:PV_{pv_len}_{j}"
                    if n_pvs > 1
                    else f"SDS:TEST:PV_{pv_len}"
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
