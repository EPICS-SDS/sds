from datetime import datetime
from typing import Any, Dict

from p4p import Value
from pydantic import model_validator

from esds.common.files import Event
from esds.common.p4p_type import P4pType


class EpicsEvent(Event):
    @model_validator(mode="before")
    def extract_values(cls, values: Dict[str, Any]):
        if "value" not in values:
            return values

        value: Value = values["value"]

        # we store all metadata from the PV structure that is not stored in any other field
        attributes = value.todict()

        # value
        values.update(
            value=value.todict("value"),
            type=P4pType(value.type()["value"]),
            data_timestamp=datetime.fromtimestamp(
                value.timeStamp.secondsPastEpoch + value.timeStamp.nanoseconds * 1e-9
            ),
        )
        attributes.pop("value")
        attributes.pop("timeStamp")

        # pulse_id
        pulse_id = value.get("pulseId")
        if pulse_id is not None:
            values.update(
                pulse_id=pulse_id.value,
                pulse_id_timestamp=datetime.fromtimestamp(
                    pulse_id.timeStamp.secondsPastEpoch
                    + pulse_id.timeStamp.nanoseconds * 1e-9
                ),
            )
        attributes.pop("pulseId")

        # eventCode
        sds_info = value.get("sdsInfo")
        if sds_info is not None:
            values.update(
                sds_event_timestamp=datetime.fromtimestamp(
                    sds_info.timeStamp.secondsPastEpoch
                    + sds_info.timeStamp.nanoseconds * 1e-9
                ),
                sds_event_pulse_id=sds_info.pulseId,
                timing_event_code=int(sds_info.evtCode),
            )

        attributes["sdsInfo"].pop("timeStamp")
        attributes["sdsInfo"].pop("pulseId")
        attributes["sdsInfo"].pop("evtCode")

        values.update(
            attributes=attributes,
        )

        return values
