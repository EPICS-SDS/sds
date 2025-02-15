from datetime import datetime
from typing import Any, Dict

from numpy import array
from p4p import Type, Value
from pydantic import model_validator

from esds.common.files import Event
from esds.common.p4p_type import P4pType

NANOS_TO_SEC = 1e-9


def _convert_timestamp(ts):
    return datetime.fromtimestamp(
        ts["secondsPastEpoch"] + ts["nanoseconds"] * NANOS_TO_SEC
    )


def calc_size(v, t):
    """
    Calculate the size of a p4p.Value object. Enums and variants are not considered.
    """
    data_size = 0
    if isinstance(t, Type):
        for elem in v:
            data_size += calc_size(v[elem], t[elem])
    else:
        data_size = array(v).nbytes
    return data_size


class EpicsEvent(Event):
    @model_validator(mode="before")
    def extract_values(cls, values: Dict[str, Any]):
        if "value" not in values:
            return values

        value: Value = values["value"]

        values.update(
            event_size=calc_size(value.todict("value"), value.type()["value"])
        )
        try:
            # we store all metadata from the PV structure that is not stored in any other field
            attributes = value.todict()

            value_type = value.type()["value"]
            value_value = attributes.pop("value")
            timestamp = attributes.pop("timeStamp")

            # value
            values.update(
                value=value_value,
                type=P4pType(value_type),
                data_timestamp=_convert_timestamp(timestamp),
            )

            # cycle_id
            cycle_id = value.get("cycleId")
            if cycle_id is not None:
                values.update(
                    cycle_id=cycle_id.value,
                    cycle_id_timestamp=_convert_timestamp(cycle_id.timeStamp),
                )
            attributes.pop("cycleId")

            # eventCode
            sds_info = value.get("sdsInfo")
            if sds_info is not None:
                values.update(
                    sds_event_timestamp=_convert_timestamp(sds_info.timeStamp),
                    sds_event_cycle_id=sds_info.cycleId,
                    timing_event_code=int(sds_info.evtCode),
                )

            attributes["sdsInfo"].pop("timeStamp")
            attributes["sdsInfo"].pop("cycleId")
            attributes["sdsInfo"].pop("evtCode")

            values.update(
                attributes=attributes,
            )

        except Exception as e:
            print("Exception in extract_values")
            print(e)
        return values
