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
        pulse_id = value.raw.get("cycleId")
        if pulse_id is not None:
            # For the moment we use the same pulse ID for the triggering event, since we have no circular buffers
            values.update(pulse_id=pulse_id, trigger_pulse_id=pulse_id)

        # eventCode
        sds_info = value.raw.get("sdsInfo")
        if sds_info is not None:
            values.update(
                trigger_date=datetime.fromtimestamp(
                    sds_info.timeStamp.secondsPastEpoch
                    + sds_info.timeStamp.nanoseconds * 1e-9
                ),
            )
        # For the moment take the acqEvt code instead of the sds code (missing)
        acq_evt = value.raw.get("acqEvt")
        if acq_evt is not None:
            values.update(
                timing_event_code=acq_evt.code,
            )
        return values
