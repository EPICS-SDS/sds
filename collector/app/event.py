from typing import Any

from pydantic import BaseModel, root_validator
from p4p import Value


def get_attribute(value: Value, name: str):
    def fn(item): return item.name == name
    iter = filter(fn, value.raw["attribute"])
    return next(iter)


class Event(BaseModel):
    name: str
    code: int
    pv_name: str
    value: Any
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
            pulse_id=value.raw.timeStamp.userTag,
        )
        # eventName
        attribute = get_attribute(value, "eventName")
        values.update(
            name=attribute["value"],
            trigger_pulse_id=attribute.timestamp.userTag,
        )
        # eventCode
        attribute = get_attribute(value, "eventCode")
        values.update(
            code=attribute.value,
            trigger_pulse_id=attribute.timestamp.userTag,
        )
        return values
