import asyncio
from datetime import datetime
from typing import List
from p4p.client.asyncio import Context


class SDSEvent:
    events = set()

    def __init__(self, name: str, pvs: List[str]) -> None:
        self.name = name
        self.pvs = set(pvs)
        self.__class__.events.add(self)

    def toJSON(self):
        return {
            "name": self.name,
            "type": self.type,
            "pvs": list(self.pvs),
        }

    @classmethod
    def get(cls, name: str) -> object:
        events = (event for event in cls.events if event.name == name)
        return next(events, None)

    @classmethod
    def get_multi(cls) -> List[object]:
        return list(cls.events)

    @classmethod
    def from_dict(cls, dict):
        type = dict.pop("type")
        subclass = next(x for x in cls.__subclasses__() if x.type == type)
        return subclass(**dict)


class DataOnDemand(SDSEvent):
    type = "dod"

    async def trigger(self):
        now = datetime.now()
        context = Context('pva')

        tasks = [next_value(context, pv, now) for pv in self.pvs]
        result = await asyncio.gather(*tasks)
        return result


class PostMortem(SDSEvent):
    type = "postmortem"


async def next_value(context, pv, now=datetime.now()):
    future = asyncio.Future()

    async def cb(value):
        if isinstance(value, Exception):
            future.set_exception(value)
        # The monitor will first call back any currently set values, the test
        # is needed to ignore them
        elif value.timestamp > now.timestamp():
            future.set_result(value)

    sub = context.monitor(pv, cb)
    # Wait for value from the callback
    result = await future
    # Close subscription once result is received
    sub.close()
    # Return the PV name and value
    return pv, result
