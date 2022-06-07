from typing import Dict, List, Literal, Optional, Set, Union
from threading import Timer
from pathlib import Path
from datetime import datetime
from functools import lru_cache
from typing_extensions import Annotated
from nexusformat.nexus import NXentry, NXdata
from pydantic import BaseModel, Field, parse_file_as, root_validator

from .logger import logger
from .config import settings


class SDSDataset(BaseModel):
    name: str
    event: "SDSEvent"
    date: datetime = Field(default_factory=datetime.utcnow)
    _entry: NXentry = Optional[NXentry]

    @root_validator
    def compute_entry(cls, values) -> Dict:
        values["_entry"] = NXentry(attrs={
            "dataset_name": values["name"],
            "event_name": values["event"].name,
            "event_type": values["event"].type,
        })
        return values

    @property
    def file_name(self):
        timestamp = self.date.strftime("%Y%m%d_%H%M%S")
        return f"{self.name}_{timestamp}.h5"

    def update(self, pulse_id, pv_name, pv_value):
        key = f"event_{pulse_id}"
        if key not in self._entry:
            self._entry[key] = NXdata(attrs={"pulse_id": pulse_id})
        self._entry[key][pv_name] = pv_value

    def write(self):
        # Create file path
        directory = Path(settings.output_dir) / \
            self.date.strftime("%Y") / self.date.strftime("%Y-%m-%d")
        path = directory / self.file_name
        # Ensure directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Event '{self.name}' writing to '{path}'")
        # Write file
        self._entry.save(path)

    def is_complete(self):
        return all(map(lambda d: set(d) == self.event.pvs, self._entry))


class SDSEvent(BaseModel):
    type: Literal["dod", "postmortem"] = Field(alias="ev_name")
    name: str = Field(alias="ds_name")
    pvs: Set[str] = Field(alias="pv_list")
    _dataset: Optional[SDSDataset] = None
    _timer: Optional[Timer] = None

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def update(self, pulse_id, pv_name, pv_value):
        # If the PV does not belong to this event, ignore it.
        if pv_name not in self.pvs:
            return
        # Start a timer to call a function to write values after a given time
        # if not all PVs have been received.
        if self._dataset is None:
            self._dataset = SDSDataset(name=self.name, event=self)

            self._timer = Timer(settings.monitor_timeout, self.timeout)
            self._timer.start()

        self._dataset.update(pulse_id, pv_name, pv_value)
        if self._dataset.is_complete():
            self.finish()

    def timeout(self):
        logger.debug(f"Event '{self.name}' timed out")
        self.finish()

    def finish(self):
        self._timer.cancel()
        logger.debug(f"Event '{self.name}' done")
        self._dataset.write()
        self._dataset = None

    @classmethod
    def get(cls, name: str) -> object:
        events = (event for event in get_events() if event.name == name)
        return next(events, None)

    @classmethod
    def get_multi(cls) -> List[object]:
        return list(get_events())

    @classmethod
    def get_all_pvs(cls):
        pvs = set()
        for event in get_events():
            pvs |= event.pvs
        return pvs


SDSDataset.update_forward_refs()


class DataOnDemand(SDSEvent):
    type: Literal["dod"] = Field(alias="ev_name")


class PostMortem(SDSEvent):
    type: Literal["postmortem"] = Field(alias="ev_name")


@lru_cache()
def get_events():
    logger.info("Loading events...")
    events = []
    m = Annotated[
        Union[DataOnDemand, PostMortem],
        Field(discriminator="type"),
    ]
    for event in parse_file_as(List[m], settings.events_file_path):
        events.append(event)
        logger.debug("Event '%s' loaded", event.name)
    return events
