import asyncio
import json
import os.path
from datetime import datetime
from multiprocessing import Process
from pathlib import Path
from typing import Optional

import pytest
from h5py import File
from p4p.client.asyncio import Context, timesout
from p4p.client.thread import Context as ThreadedContext
from pydantic import TypeAdapter
from tests.functional.service_loader import collector_service, indexer_service
from tests.test_ioc.pva_server import run_server

from esds.collector.collector_manager import CollectorManager
from esds.collector.config import settings
from esds.collector.main import load_collectors, main, wait_for_indexer
from esds.common.files import CollectorDefinition
from esds.common.files.collector import CollectorList
from esds.common.files.config import settings as file_settings


@pytest.mark.usefixtures("indexer_service", "collector_service")
class TestCollector:
    @classmethod
    def setup_class(cls):
        n_pvs = 10
        n_elem = 100
        prefix = "SDS:TEST:"

        cls.ioc_process = Process(target=run_server, args=[n_pvs, n_elem, prefix])
        cls.ioc_process.start()

        # Waiting to connect to the SDS:TEST:TRIG, which is the last one to be created
        with ThreadedContext() as ctxt:
            try:
                ctxt.get("SDS:TEST:TRIG", timeout=15)

                pv_len = 100
                n_pvs = 10
                cls.test_pv = f"SDS:TEST:PV_{pv_len}_0"
                ctxt.put("SDS:TEST:N_ELEM", pv_len)
                ctxt.put("SDS:TEST:N_PVS", n_pvs)
            except TimeoutError:
                TestCollector.teardown_class()
                raise RuntimeError("Timeout waiting for test IOC to start...")

    @classmethod
    def teardown_class(cls):
        cls.ioc_process.terminate()
        cls.ioc_process.join()

    @timesout()
    async def trigger(self, timeout=10):
        with Context() as ctxt:
            await ctxt.put("SDS:TEST:TRIG", True)

    @timesout()
    async def set_n_cycles(self, n_cycles, timeout=10):
        with Context() as ctxt:
            await ctxt.put("SDS:TEST:N_CYCLES", n_cycles)

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

    async def sds_event_n_cycles(self, n: int, same_event: bool = True):
        n_cycles = n if same_event else 1
        await self.set_n_cycles(n_cycles)

        first_cycle = await self.get_count() + 1
        last_cycle = first_cycle - 1 + n
        mon, queue = await self.wait_for_pv_value(last_cycle)
        n_files = 1 if same_event else n
        for _i in range(n_files):
            await self.trigger()
        value = await asyncio.wait_for(queue.get(), 15)
        assert (
            value[0] == last_cycle
        ), f"Last cycle not received by collector. Last cycle={last_cycle}. Last received={value[0]}"
        mon.close()
        # Waiting for 5 more seconds than the flush to file delay to make sure the files are written to disk
        await asyncio.sleep(settings.flush_file_delay + 5)

        # Check files
        collectors_path = settings.collector_definitions
        with open(collectors_path, "r") as settings_file:
            collectors = TypeAdapter(Optional[CollectorList]).validate_python(
                json.load(settings_file)
            )

        for collector in collectors:
            # Skip never triggered event
            if collector.event_code != 1:
                continue
            directory = Path(
                datetime.fromtimestamp(value.timestamp).strftime("%Y"),
                datetime.fromtimestamp(value.timestamp).strftime("%Y-%m-%d"),
            )

            pv_list = await self.get_pv_list()

            for n in range(n_files):
                sds_cycle = int(first_cycle) + n_cycles - 1
                collector_full_name = os.path.join(
                    collector.parent_path, collector.name
                )
                file_path = (
                    file_settings.storage_path
                    / directory
                    / (
                        f"{collector_full_name.lstrip('/').replace('/','_')}_{collector.event_code}_{sds_cycle+n}.h5"
                    )
                )
                assert file_path.exists(), f"File {file_path} not found."

                h5file = File(file_path, "r")
                entry = h5file.get("entry", None)
                assert (
                    entry is not None
                ), f"File {file_path} does not contain an entry group."
                sds_event = entry.get(f"sds_event_{sds_cycle+n}", None)
                assert (
                    sds_event is not None
                ), f"File {file_path} does not contain the sds event for cycle {sds_cycle+n}."

                for i in range(n_cycles):
                    cycle = sds_event.get(f"cycle_{int(first_cycle)+n+i}", None)
                    assert (
                        cycle is not None
                    ), f"File {file_path} does not contain the cycle {int(first_cycle)+n +i}."

                    for pv in collector.pvs:
                        if pv in pv_list:
                            pv_field = cycle.get(pv, None)
                            assert (
                                pv_field is not None
                            ), f"PV {pv} not found in file {file_path} for cycle {int(first_cycle)+n+i} ({i+1}/{n_cycles})"

                h5file.close()

    async def test_sds_event_1_cycle(self):
        await self.sds_event_n_cycles(1)

    async def test_sds_event_3_cycles(self):
        await self.sds_event_n_cycles(3)

    async def test_sds_event_3_independent_cycles(self):
        await self.sds_event_n_cycles(3, same_event=False)

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

    async def test_main_and_wait(self):
        task = asyncio.create_task(self.main_no_cancelled_error())
        await asyncio.sleep(1)
        task.cancel()
        await task
