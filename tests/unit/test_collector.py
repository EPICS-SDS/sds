from datetime import datetime

import pytest
from sds.collector.collector import Collector
from sds.common.files import AcqEvent, AcqInfo, BeamInfo, Dataset, Event

collector = Collector(
    name="test_collector",
    event_name="test_event",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    id="test_id",
    host="0.0.0.0",
)

acq_info = AcqInfo(
    acq_type="",
    id=0,
)

acq_event = AcqEvent(
    timestamp=datetime.utcnow(),
    name="TestEvent",
    delay=0.0,
    code=0,
    evr="TestEVR",
)

beam_info = BeamInfo(
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
    timing_event_code=2,
    data_timestamp=datetime.utcnow(),
    sds_event_timestamp=datetime.utcnow(),
    pulse_id=1,
    sds_event_pulse_id=1,
    acq_info=acq_info,
    acq_event=acq_event,
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
            sds_event_timestamp=datetime.utcnow(),
            sds_event_pulse_id=event.sds_event_pulse_id,
            path=f"{collector.name}_{event.timing_event_code}_{event.sds_event_pulse_id}",
            acq_info=acq_info,
            acq_event=acq_event,
            beam_info=beam_info,
        )

        await dataset.index("")
