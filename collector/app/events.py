class SDSEvent:
    events = set()

    def __init__(self, name: str, pvs: List[str]) -> None:
        self.name = name
        self.pvs = set(pvs)
        self.__class__.events.add(self)

    @classmethod
    def from_dict(cls, dict):
        type = dict.pop("type")
        subclass = next(x for x in cls.__subclasses__() if x.type == type)
        return subclass(**dict)


class DataOnDemand(SDSEvent):
    type = "dod"


class PostMortem(SDSEvent):
    type = "postmortem"

