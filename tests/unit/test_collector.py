from datetime import UTC, datetime

import pytest

from esds.collector.collector import Collector
from esds.common.files import Dataset, Event

collector = Collector(
    name="test_collector",
    parent_path="/",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    collector_id="test_id",
)

acq_event = dict(
    timestamp=datetime.now(UTC),
    name="TestEvent",
    delay=0.0,
    code=0,
    evr="TestEVR",
)

beam_info = dict(
    mode="TestMode",
    state="ON",
    present="YES",
    len=2.86e-3,
    energy=2e9,
    dest="Target",
    curr=62.5e-3,
)

event = Event(
    pv_name="TEST:PV:3",
    value=1,
    type=None,
    timing_event_code=2,
    data_timestamp=datetime.now(UTC),
    sds_event_timestamp=datetime.now(UTC),
    sds_cycle_start_timestamp=datetime.now(UTC),
    cycle_id_timestamp=datetime.now(UTC),
    cycle_id=1,
    sds_event_cycle_id=1,
    attributes=dict(acq_event=acq_event, beam_info=beam_info),
    event_size=1,
)


class TestCollector:
    def test_collector_bad_event(self):
        collector.update(event)
        assert len(collector._tasks) == 0


class TestDataset:
    @pytest.mark.asyncio
    async def test_dataset_index_fail(self):
        dataset = Dataset(
            collector_id=collector.collector_id,
            sds_event_timestamp=datetime.now(UTC),
            sds_cycle_start_timestamp=datetime.now(UTC),
            sds_event_cycle_id=event.sds_event_cycle_id,
            path=f"{collector.name}_{event.timing_event_code}_{event.sds_event_cycle_id}",
            acq_event=acq_event,
            beam_info=beam_info,
        )

        await dataset.index("")
