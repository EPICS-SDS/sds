import asyncio
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List

import pytest
from collector.collector import CollectorSchema
from collector.collector_manager import CollectorManager
from collector.config import settings
from collector.main import load_collectors, main
from common.files.config import settings as file_settings
from nexusformat.nexus import NXFile
from p4p.client.asyncio import Context, timesout
from p4p.client.thread import Context as ThContext
from pydantic import parse_file_as
from tests.functional.service_loader import collector_service, indexer_service


class TestCollector:
    @pytest.fixture(autouse=True)
    def _start_collector_service(self, indexer_service, collector_service):
        pass

    @classmethod
    def setup_class(cls):
        cls.p = subprocess.Popen(
            ["python", "test_ioc/pva_server.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Waiting to connect to the SDS:TEST:TRIG, which is the last one to be created
        ctxt = ThContext()
        ctxt.get("SDS:TEST:TRIG")

        pv_len = 100
        n_pvs = 10
        cls.test_pv = f"SDS:TEST:PV_{pv_len}_0"
        ctxt.put("SDS:TEST:N_ELEM", pv_len)
        ctxt.put("SDS:TEST:N_PVS", n_pvs)
        ctxt.close()

    @classmethod
    def teardown_class(cls):
        cls.p.kill()

    @pytest.mark.asyncio
    @timesout()
    async def trigger(self, timeout=10):
        with Context() as ctxt:
            await ctxt.put("SDS:TEST:TRIG", True)

    @pytest.mark.asyncio
    @timesout()
    async def set_n_pulses(self, n_pulses, timeout=10):
        with Context() as ctxt:
            await ctxt.put("SDS:TEST:N_PULSES", n_pulses)

    async def get_count(self):
        with Context() as ctxt:
            value = await asyncio.wait_for(ctxt.get(self.test_pv), 5)
        return value[0]

    async def get_pv_list(self):
        with Context() as ctxt:
            value = await asyncio.wait_for(ctxt.get("SDS:TEST:PVS"), 5)
        return value

    async def wait_for_pv_value(self, new_value):
        queue = asyncio.Queue()
        mon = None

        async def cb(value):
            if value[0] == new_value:
                await queue.put(value)
                if mon is not None:
                    mon.close()

        ctxt = Context()
        mon = ctxt.monitor(self.test_pv, cb=cb)
        return mon, queue

    async def trigger_n_pulses(self, n: int):
        await self.set_n_pulses(n)

        first_pulse = await self.get_count() + 1
        last_pulse = await self.get_count() + n
        mon, queue = await self.wait_for_pv_value(last_pulse)
        await self.trigger()
        value = await asyncio.wait_for(queue.get(), 5)
        assert value[0] == last_pulse
        mon.close()
        # Waiting one more second than the collector timeout to make sure the files are written to disk
        await asyncio.sleep(settings.collector_timeout + 1)

        # Check files
        collectors_path = settings.collector_definitions
        for collector in parse_file_as(List[CollectorSchema], collectors_path):
            # Skip never triggered event
            if collector.event_code != 1:
                continue
            directory = Path(
                datetime.fromtimestamp(value.timestamp).strftime("%Y"),
                datetime.fromtimestamp(value.timestamp).strftime("%Y-%m-%d"),
            )
            file_path = (
                file_settings.storage_path
                / directory
                / (collector.name + f"_{int(first_pulse)}.h5")
            )
            assert file_path.exists()

            nx = NXFile(file_path, "r")
            root = nx.readfile()
            entry = root.entries.get("entry")
            assert entry is not None
            trigger = entry.entries.get(f"trigger_{int(first_pulse)}")
            assert trigger is not None

            pv_list = await self.get_pv_list()

            for n in range(n):
                pulse = trigger.entries.get(f"pulse_{int(first_pulse)+n}")
                assert pulse is not None

                for pv in collector.pvs:
                    if pv in pv_list:
                        pv_field = pulse.entries.get(pv)
                        assert pv_field is not None

            nx.close()

    @pytest.mark.asyncio
    async def test_trigger_1_pulse(self):
        await self.trigger_n_pulses(1)

    @pytest.mark.asyncio
    async def test_trigger_3_pulse(self):
        await self.trigger_n_pulses(3)

    @pytest.mark.asyncio
    async def test_collector_manager_as_context_manager(self):
        collectors = await load_collectors()
        async with CollectorManager(collectors) as cm:
            await cm.wait_for_startup()

    async def main_no_cancelled_error(self):
        try:
            await main()
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_main_and_wait(self):
        task = asyncio.create_task(self.main_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task
