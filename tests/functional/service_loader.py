from indexer import app as indexer_app
from retriever import app as retriever_app
from collector.main import load_collectors
from collector.collector_manager import CollectorManager
import uvicorn
import asyncio
from typing import List, Optional
import pytest_asyncio

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
        super().__init__(config=uvicorn.Config(app, host=host, port=port))

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
    """Start server as test fixture and tear down after test"""
    server = UvicornTestServer(indexer_app, port=INDEXER_PORT)
    await server.up()
    yield
    await server.down()


@pytest_asyncio.fixture()
async def retriever_service():
    """Start server as test fixture and tear down after test"""
    server = UvicornTestServer(retriever_app, port=RETRIEVER_PORT)
    await server.up()
    yield
    await server.down()


@pytest_asyncio.fixture()
async def collector_service():
    """Start server as test fixture and tear down after test"""

    collectors = await load_collectors()
    print("Starting collectors...")

    cm = CollectorManager(collectors)
    cm.start()
    await cm.wait_for_startup()
    yield
    cm.close()
    await cm.join()
