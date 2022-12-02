import pytest
from datetime import datetime

from collector.collector import Collector
from collector.indexable_dataset import IndexableDataset
from common.files import Event

collector = Collector(
    name="test_collector",
    event_name="test_event",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    id="test_id",
)
event = Event(
    pv_name="TEST:PV:3",
    value=1,
    timing_event_code=2,
    data_date=datetime.utcnow(),
    trigger_date=datetime.utcnow(),
    pulse_id=1,
    trigger_pulse_id=1,
)


class TestCollector:
    def test_collector_bad_event(self):

        collector.update(event)
        assert len(collector._tasks) == 0


class TestIndexableDataset:
    @pytest.mark.asyncio
    async def test_dataset_index_fail(self):
        dataset = IndexableDataset(
            collector_id=collector.id,
            collector_name=collector.name,
            trigger_date=datetime.utcnow(),
            trigger_pulse_id=event.trigger_pulse_id,
            data_date=[datetime.utcnow()],
            data_pulse_id=[event.trigger_pulse_id],
            event_code=event.timing_event_code,
        )

        await dataset.index()
