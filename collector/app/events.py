from typing import List


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


class PostMortem(SDSEvent):
    type = "postmortem"

