import pytest
from datetime import datetime

from collector.collector import Collector
from common.files import Dataset, Event, BeamInfo

collector = Collector(
    name="test_collector",
    event_name="test_event",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    id="test_id",
    host="0.0.0.0",
)

beam_info = BeamInfo(
    mode="TestMode",
    state="ON",
    present="YES",
    len=2.84e-3,
    energy=2e9,
    dest="Target",
    curr=62.5e-3,
)

event = Event(
    pv_name="TEST:PV:3",
    value=1,
    timing_event_code=2,
    data_timestamp=datetime.utcnow(),
    trigger_timestamp=datetime.utcnow(),
    pulse_id=1,
    trigger_pulse_id=1,
    beam_info=beam_info,
)


class TestCollector:
    def test_collector_bad_event(self):

        collector.update(event)
        assert len(collector._tasks) == 0


class TestDataset:
    @pytest.mark.asyncio
    async def test_dataset_index_fail(self):
        dataset = Dataset(
            collector_id=collector.id,
            trigger_timestamp=datetime.utcnow(),
            trigger_pulse_id=event.trigger_pulse_id,
            path=f"{collector.name}_{event.timing_event_code}_{event.trigger_pulse_id}",
            beam_info=beam_info,
        )

        await dataset.index("")
