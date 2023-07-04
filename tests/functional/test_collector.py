import asyncio
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List

import pytest
from esds.collector.collector_manager import CollectorManager
from esds.collector.config import settings
from esds.collector.main import load_collectors, main, wait_for_indexer
from esds.common.files.config import settings as file_settings
from esds.common.files import CollectorDefinition

from h5py import File
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
        ctxt.get("SDS:TEST:TRIG", timeout=15)

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

    async def sds_event_n_pulses(self, n: int, same_event: bool = True):
        n_pulses = n if same_event else 1
        await self.set_n_pulses(n_pulses)

        first_pulse = await self.get_count() + 1
        last_pulse = first_pulse - 1 + n
        mon, queue = await self.wait_for_pv_value(last_pulse)
        n_files = 1 if same_event else n
        for _i in range(n_files):
            await self.trigger()
        value = await asyncio.wait_for(queue.get(), 15)
        assert (
            value[0] == last_pulse
        ), f"Last pulse not received by collector. Last pulse={last_pulse}. Last received={value[0]}"
        mon.close()
        # Waiting for 5 more seconds than the flush to file delay to make sure the files are written to disk
        await asyncio.sleep(settings.flush_file_delay + 5)

        # Check files
        collectors_path = settings.collector_definitions
        for collector in parse_file_as(List[CollectorDefinition], collectors_path):
            # Skip never triggered event
            if collector.event_code != 1:
                continue
            directory = Path(
                datetime.fromtimestamp(value.timestamp).strftime("%Y"),
                datetime.fromtimestamp(value.timestamp).strftime("%Y-%m-%d"),
            )

            pv_list = await self.get_pv_list()

            for n in range(n_files):
                file_path = (
                    file_settings.storage_path
                    / directory
                    / (
                        f"{collector.name}_{collector.event_code}_{int(first_pulse)+n}.h5"
                    )
                )
                assert file_path.exists(), f"File {file_path} not found."

                h5file = File(file_path, "r")
                entry = h5file.get("entry", None)
                assert (
                    entry is not None
                ), f"File {file_path} does not contain an entry group."
                sds_event = entry.get(f"sds_event_{int(first_pulse)+n}", None)
                assert (
                    sds_event is not None
                ), f"File {file_path} does not contain the sds event for pulse {int(first_pulse)+n}."

                for i in range(n_pulses):
                    pulse = sds_event.get(f"pulse_{int(first_pulse)+n+i}", None)
                    assert (
                        pulse is not None
                    ), f"File {file_path} does not contain the pulse {int(first_pulse)+n +i}."

                    for pv in collector.pvs:
                        if pv in pv_list:
                            pv_field = pulse.get(pv, None)
                            assert (
                                pv_field is not None
                            ), f"PV {pv} not found in file {file_path} for pulse {int(first_pulse)+n+i} ({i+1}/{n_pulses})"

                h5file.close()

    @pytest.mark.asyncio
    async def test_sds_event_1_pulse(self):
        await self.sds_event_n_pulses(1)

    @pytest.mark.asyncio
    async def test_sds_event_3_pulses(self):
        await self.sds_event_n_pulses(3)

    @pytest.mark.asyncio
    async def test_sds_event_3_independent_pulses(self):
        await self.sds_event_n_pulses(3, same_event=False)

    @pytest.mark.asyncio
    async def test_collector_manager_as_context_manager(self):
        await wait_for_indexer()
        collectors = await load_collectors()
        async with await CollectorManager.create(collectors) as cm:
            await cm.wait_for_startup()


class TestCollectorServer:
    async def main_no_cancelled_error(self):
        try:
            await main(root_path="", reload=False)
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_main_and_wait(self):
        task = asyncio.create_task(self.main_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task
