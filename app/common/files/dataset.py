from datetime import datetime
from pathlib import Path
from typing import List

from common.files.event import Event
from nexusformat.nexus import NXdata, NXentry
from pydantic import BaseModel, root_validator

from common.files.config import settings


class DatasetSchema(BaseModel):
    """
    Model for a file containing Event objects that belong to the same timing
    event (same trigger_pulse_id) and share the same collector.
    """

    collector_id: str
    trigger_date: datetime
    trigger_pulse_id: int
    path: Path
    data_date: List[datetime]
    data_pulse_id: List[int]


class Dataset(DatasetSchema):
    name: str
    entry: NXentry

    class Config:
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def extract_path(cls, values):
        name = values["collector_name"] + "_" + str(values["trigger_pulse_id"])
        values.update(name=name)

        directory = Path(
            values["trigger_date"].strftime("%Y"),
            values["trigger_date"].strftime("%Y-%m-%d"),
        )
        path = directory / f"{name}.h5"
        values.update(path=path)

        entry = NXentry(
            attrs={
                "collector_name": values["collector_name"],
                "event_code": values["event_code"],
            }
        )
        values.update(entry=entry)
        return values

    def update(self, event: Event):
        """
        Add an event to the NeXus file
        """
        trigger_key = f"trigger_{event.trigger_pulse_id}"
        pulse_key = f"pulse_{event.pulse_id}"
        if trigger_key not in self.entry:
            self.entry[trigger_key] = NXentry(
                attrs={
                    "trigger_pulse_id": event.trigger_pulse_id,
                    "trigger_timestamp": event.trigger_date.isoformat(),
                }
            )
        if pulse_key not in self.entry[trigger_key]:
            self.entry[trigger_key][pulse_key] = NXdata(
                attrs={
                    "pulse_id": event.pulse_id,
                    "timestamp": event.data_date.isoformat(),
                }
            )
        self.entry[trigger_key][pulse_key][event.pv_name] = event.value
        self.data_date.append(event.data_date)
        self.data_pulse_id.append(event.pulse_id)

    async def write(self):
        """
        Write NeXus file into storage
        """
        try:
            print(repr(self), f"writing to '{self.path}'")
            absolute_path = settings.storage_path / self.path
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            self.entry.save(absolute_path, mode="w")
            print(repr(self), "writing done.")
        except Exception as e:
            print(repr(self), "writing failed!")
            print(e)

    def __repr__(self):
        return f"Dataset({self.name})"
