from typing import List
from threading import Timer

from .logger import logger


class SDSEvent:
    events = set()

    def __init__(self, name: str, pvs: List[str]) -> None:
        self.__class__.events.add(self)
        self.name = name
        self.pvs = set(pvs)
        self.values = None
        self.pulse_id = None
        self.timer = None

    def update(self, pulse_id, pv, value):
        # If the PV does not belong to this event, ignore it.
        if pv not in self.pvs:
            return
        # Start a timer to call a function to write values after a given time
        # if not all PVs have been received.
        if self.timer is None or not self.timer.is_alive():
            self.pulse_id = pulse_id
            self.values = {}

            self.timer = Timer(2, self.timeout)
            self.timer.start()
        # Ignore any subsequent pulses until the first is finished.
        if self.pulse_id != pulse_id:
            logger.info("Event '%s' (%d) received conflicting pulse ID %d",
                        self.name, self.pulse_id, pulse_id)
        else:
            self.values[pv] = value
            if set(self.values.keys()) == self.pvs:
                self.write()

    def timeout(self):
        logger.debug("Event '%s' (%d) timed out with %d/%d pvs",
                     self.name, self.pulse_id, len(self.values), len(self.pvs))
        self.write()

    def write(self):
        self.timer.cancel()
        logger.debug("Event '%s' (%d) writing", self.name, self.pulse_id)

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
    def get_all_pvs(cls):
        pvs = set()
        for event in cls.get_multi():
            pvs |= event.pvs
        return pvs

    @classmethod
    def from_dict(cls, dict):
        type = dict.pop("type")
        subclass = next(x for x in cls.__subclasses__() if x.type == type)
        return subclass(**dict)


class DataOnDemand(SDSEvent):
    type = "dod"


class PostMortem(SDSEvent):
    type = "postmortem"
