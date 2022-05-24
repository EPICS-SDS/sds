from typing import List
from threading import Timer
from pathlib import Path
from datetime import datetime
from nexusformat.nexus import NXroot, NXentry, NXdata

from .logger import logger
from .config import settings


class SDSEvent:
    events = set()

    def __init__(self, name: str, pvs: List[str]) -> None:
        self.__class__.events.add(self)
        self.name = name
        self.pvs = set(pvs)
        self.values = None
        self.pulse_id = None
        self.timer = None

    def update(self, pv, pulse_id, value):
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
                self.finish()

    def timeout(self):
        logger.debug("Event '%s' (%d) timed out with %d/%d pvs",
                     self.name, self.pulse_id, len(self.values), len(self.pvs))
        self.finish()

    def finish(self):
        self.timer.cancel()
        logger.debug("Event '%s' (%d) done", self.name, self.pulse_id)
        self.write()

    def write(self):
        date = datetime.utcnow()
        # Create file
        entry = NXentry(
            dataset_name=self.name,
            event_name=self.name,
            event_type=self.type)
        data = NXdata(
            pulse_id=self.pulse_id)
        for pv, value in self.values.items():
            data[pv] = value
        entry[f"event_pulseID_{self.pulse_id}"] = data
        # Create file path
        directory = Path(settings.output_dir) / \
            date.strftime("%Y") / date.strftime("%Y-%m-%d")
        file_name = self.name + "_" + date.strftime("%Y%m%d_%H%M%S") + ".h5"
        path = directory / file_name
        # Ensure directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug("Event '%s' (%d) writing to '%s'",
                     self.name, self.pulse_id, path)
        # Write file
        entry.save(path)

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
