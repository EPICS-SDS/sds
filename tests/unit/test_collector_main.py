import asyncio
from asyncio import CancelledError

import pytest
from sds.collector.config import settings
from sds.collector.main import load_collectors, main, wait_for_indexer


class TestCollectorMain:
    async def wait_for_indexer_no_cancelled_error(self):
        try:
            await wait_for_indexer()
        except CancelledError:
            pass

    async def main_no_cancelled_error(self):
        try:
            await main()
        except CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_load_collectors(self):
        collectors = await load_collectors()
        assert len(collectors) == 4
        assert collectors[0].name == "test"

    @pytest.mark.asyncio
    async def test_main_and_wait(self):
        settings.wait_for_indexer = True
        task = asyncio.create_task(self.main_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task
