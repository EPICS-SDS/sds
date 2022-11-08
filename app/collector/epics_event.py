from datetime import datetime
from typing import Any

from p4p import Value
from pydantic import root_validator
from common.files import Event


class EpicsEvent(Event):
    @root_validator(pre=True)
    def extract_values(cls, values: Value):
        if "value" not in values:
            return values
        value = values["value"]
        # value
        values.update(
            value=value,
            data_date=datetime.fromtimestamp(
                value.raw.timeStamp.secondsPastEpoch
                + value.raw.timeStamp.nanoseconds * 1e-9
            ),
        )

        # pulse_id
        pulse_id = value.raw.get("pulseId")
        if pulse_id is not None:
            # For the moment we use the same pulse ID for the triggering event, since we have no circular buffers
            values.update(pulse_id=pulse_id.value)

        # eventCode
        sds_info = value.raw.get("sdsInfo")
        if sds_info is not None:
            values.update(
                trigger_date=datetime.fromtimestamp(
                    sds_info.timeStamp.secondsPastEpoch
                    + sds_info.timeStamp.nanoseconds * 1e-9),
                trigger_pulse_id = sds_info.pulseId,
                timing_event_code=int(sds_info.evtCode),
                timing_event_name='test',
            )
        return values
