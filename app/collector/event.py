from datetime import datetime
from typing import Any

from p4p import Value
from pydantic import BaseModel, root_validator


def get_attribute(value: Value, name: str):
    def fn(item):
        return item.name == name

    iterator = filter(fn, value.raw["attribute"])
    return next(iterator, None)


class Event(BaseModel):
    name: str
    code: int
    pv_name: str
    value: Any
    data_date: datetime
    trigger_date: datetime
    pulse_id: int
    trigger_pulse_id: int

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
            pulse_id=value.raw.timeStamp.userTag,
        )
        # eventName
        attribute = get_attribute(value, "eventName")
        if attribute is not None:
            values.update(
                name=attribute["value"],
                trigger_date=datetime.fromtimestamp(
                    value.raw.timeStamp.secondsPastEpoch
                    + value.raw.timeStamp.nanoseconds * 1e-9
                ),
                trigger_pulse_id=attribute.timestamp.userTag,
            )
        # eventCode
        attribute = get_attribute(value, "eventCode")
        if attribute is not None:
            values.update(
                code=attribute.value,
                trigger_pulse_id=attribute.timestamp.userTag,
            )
        return values
