import asyncio
from asyncio import CancelledError

import pytest
from sds.common.db.connection import wait_for_connection
from sds.common.db.config import settings


class TestConnection:
    async def wait_for_connection_no_cancelled_error(self, **kwargs):
        try:
            await wait_for_connection(**kwargs)
        except CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_wait_for_connection_ok(self):
        settings.elastic_url = "http://elasticsearch:9200"
        task = asyncio.create_task(self.wait_for_connection_no_cancelled_error())
        await asyncio.sleep(1)
        assert task.done() is True
        task.cancel()
        await task

    @pytest.mark.asyncio
    async def test_wait_for_connection_nok(self):
        settings.elastic_url = "http://bad.url:9200"
        task = asyncio.create_task(self.wait_for_connection_no_cancelled_error())
        await asyncio.sleep(1)
        assert task.done() is False
        task.cancel()
        await task

    @pytest.mark.asyncio
    async def test_wait_for_connection_timeout(self):
        with pytest.raises(SystemExit):
            settings.elastic_url = "http://bad.url:9200"
            await wait_for_connection(timeout=2)
