import asyncio
from asyncio import CancelledError

import pytest
from aiohttp.client_exceptions import ClientConnectorError
from collector.config import settings
from collector.main import load_collectors, main, wait_for_indexer


class TestCollectorMain:
    async def load_collector_no_cancelled_error(self):
        try:
            await wait_for_indexer()
            await load_collectors()
        except CancelledError:
            pass

    async def main_no_cancelled_error(self):
        try:
            await main()
        except CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_load_collectors_and_wait(self):
        settings.wait_for_indexer = True
        task = asyncio.create_task(self.load_collector_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task

    @pytest.mark.asyncio
    async def test_load_collectors_no_wait(self):
        settings.wait_for_indexer = False
        with pytest.raises(ClientConnectorError):
            await load_collectors()

    @pytest.mark.asyncio
    async def test_main_and_wait(self):
        settings.wait_for_indexer = True
        task = asyncio.create_task(self.main_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task
