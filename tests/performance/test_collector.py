import asyncio
import time
from datetime import datetime
from pathlib import Path
from typing import List

import numpy as np
import pytest
from collector.config import settings
from common.files.config import settings as file_settings
from common.schemas import CollectorBase
from nexusformat.nexus import NXFile
from p4p.client.asyncio import Context, timesout
from p4p.client.thread import Context as ThContext
from pydantic import parse_file_as

from tests.functional.service_loader import (
    configurable_collector_service,
    indexer_service,
)


class TestCollector:
    @pytest.fixture(autouse=True)
    def _start_indexer_service(self, indexer_service):
        pass

    @classmethod
    def setup_class(cls):
        cls.ctxt = Context()
        with open("output.csv", "w") as out_file:
            out_file.writelines(
                "data rate [Mbps], real data rate [Mbps], freq [Hz], real freq [Hz], success rate [%], # updates received,  # updates expected, # PVs, PV length, # pulses, # triggers, test time [s] \n"
            )

        # Waiting to connect to the SDS:TEST:TRIG, which is the last one to be created
        ctxt = ThContext()
        ctxt.get("SDS:TEST:TRIG")
        ctxt.close()

    @classmethod
    def teardown_class(cls):
        cls.ctxt.close()
        # cls.p.kill()

    @pytest.mark.asyncio
    @timesout()
    async def trigger(self):
        await self.ctxt.put("SDS:TEST:TRIG", True)

    @pytest.mark.asyncio
    @timesout(deftimeout=10)
    async def configure_ioc(self, n_pulses=None, pv_len=None, n_pvs=None):
        if pv_len is not None:
            await self.ctxt.put("SDS:TEST:N_ELEM", pv_len)
        if n_pvs is not None:
            await self.ctxt.put("SDS:TEST:N_PVS", n_pvs)
        if n_pulses is not None:
            await self.ctxt.put("SDS:TEST:N_PULSES", n_pulses)
        await asyncio.sleep(5)

    async def get_count(self):
        value = await asyncio.wait_for(self.ctxt.get(self.test_pv), 5)
        return value[0]

    async def get_pv_list(self):
        value = await asyncio.wait_for(self.ctxt.get("SDS:TEST:PVS"), 5)
        return value

    async def wait_for_pv_value(self, new_value):
        queue = asyncio.Queue()
        mon = None

        async def cb(value):
            if value[0] == new_value:
                await queue.put(value)

        mon = self.ctxt.monitor(self.test_pv, cb=cb)
        return mon, queue

    async def single_test(
        self, n_pulses: int, n_pvs: int, pv_len: int, freq: float, n_triggers: int
    ):
        first_pulse, last_pulse, timestamp = await self.trigger_ioc_updates(
            n_pulses, n_pvs, pv_len, freq, n_triggers
        )

        # Waiting one more second than the collector timeout to make sure the files are written to disk
        await asyncio.sleep(settings.collector_timeout + 5)

        directory = Path(
            datetime.fromtimestamp(timestamp).strftime("%Y"),
            datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d"),
        )

        pv_updates_collected, elapsed = await self.check_output(
            n_pulses, directory, first_pulse, n_triggers
        )

        return pv_updates_collected, elapsed

    async def trigger_ioc_updates(
        self, n_pulses: int, n_pvs: int, pv_len: int, freq: float, n_triggers: int
    ):
        self.test_pv = f"SDS:TEST:PV_{pv_len}"
        if n_pvs > 1:
            self.test_pv += "_0"

        first_pulse = await self.get_count() + 1
        last_pulse = first_pulse + n_pulses - 1
        for i in range(n_triggers):
            t0 = time.time()
            mon, queue = await self.wait_for_pv_value(last_pulse + i * n_pulses)
            await self.trigger()
            value = await asyncio.wait_for(queue.get(), 5)
            if mon is not None:
                mon.close()
            assert value[0] == last_pulse + i * n_pulses
            mon.close()
            time_delta = n_pulses / freq - (time.time() - t0)
            if time_delta > 0:
                await asyncio.sleep(time_delta)
            else:
                print(f"time_delta={time_delta}")
        return first_pulse, last_pulse, value.timestamp

    async def check_output(
        self, n_pulses: int, directory: Path, first_pulse: int, n_triggers: int
    ):
        # Check files
        pv_updates_collected = 0
        collectors_path = settings.collector_definitions
        timestamps = []
        for collector in parse_file_as(List[CollectorBase], collectors_path):
            for i in range(n_triggers):
                try:
                    file_path = (
                        file_settings.storage_path
                        / directory
                        / (collector.name + f"_{int(first_pulse + i * n_pulses)}.h5")
                    )
                    assert file_path.exists()

                    nx = NXFile(file_path, "r")
                    root = nx.readfile()
                    entry = root.entries.get("entry")
                    assert entry is not None
                    trigger = entry.entries.get(
                        f"trigger_{int(first_pulse + i * n_pulses)}"
                    )
                    assert trigger is not None

                    timestamps.append(
                        datetime.fromisoformat(trigger.attrs["trigger_timestamp"])
                    )

                    pv_list = await self.get_pv_list()

                    for n in range(n_pulses):
                        try:
                            pulse = trigger.entries.get(
                                f"pulse_{int(first_pulse + i * n_pulses)+n}"
                            )
                            assert pulse is not None

                            for pv in collector.pvs:
                                if pv in pv_list:
                                    pv_field = pulse.entries.get(pv)
                                    if pv_field is not None:
                                        pv_updates_collected += 1
                                    # assert pv_field is not None, f"PV {pv} not present in file {file_path.name}"
                        except AssertionError:
                            pass

                    nx.close()
                except AssertionError:
                    pass

        elapsed = (np.max(timestamps) - np.min(timestamps)).total_seconds()
        return pv_updates_collected, elapsed

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "n_pvs, pv_len , n_pulses",
        [
            (
                n_pvs,
                int(pv_len / n_pvs),
                n_pulses,
            )
            for n_pvs in [1, 10, 20, 50]
            for pv_len in [1e4, 1e5, 1e6]
            for n_pulses in [1, 5, 10]
        ],
    )
    async def test_parametric(
        self,
        configurable_collector_service,
        n_pvs,
        pv_len,
        n_pulses,
        freq=14,
        n_triggers=50,
    ):
        await self.configure_ioc(n_pulses=n_pulses, pv_len=pv_len, n_pvs=n_pvs)

        configurable_collector_service.generate_collector_definitions_file(
            n_pvs=n_pvs, pv_len=pv_len, n_collectors=1
        )
        await configurable_collector_service.start()

        n_triggers = int(n_triggers / n_pulses)

        pv_updates_collected, elapsed = await self.single_test(
            n_pulses=n_pulses,
            n_pvs=n_pvs,
            pv_len=pv_len,
            freq=freq,
            n_triggers=n_triggers,
        )

        data_rate = n_pvs * pv_len * freq * 1e-6 * 8 * 8
        real_freq = n_pulses * (n_triggers - 1) / elapsed
        data_rate_real = n_pvs * pv_len * real_freq * 1e-6 * 8 * 8
        success_rate = pv_updates_collected / (n_pvs * n_pulses * n_triggers)
        with open("output.csv", "a") as out_file:
            out_file.writelines(
                f"{data_rate}, {data_rate_real:.3f}, {freq}, {real_freq:.3f}, {success_rate*100:.3f}, {pv_updates_collected}, {n_pvs * n_pulses * n_triggers}, {n_pvs}, {pv_len}, {n_pulses},{n_triggers}, {elapsed:.3f}\n"
            )
        # Tests won't fail, this is only to measure performance
        assert True
