from pathlib import Path
from typing import Dict, List

from common.files.config import settings
from common.files.dataset import Dataset
from common.files.event import Event
from pydantic import BaseModel, root_validator
from h5py import File


class NexusFile(BaseModel):
    """
    Model for a file containing Dataset objects from one or more timing
    event that share the same collector.
    """

    collector_id: str
    path: Path
    name: str
    events: List[Event]
    datasets: Dict[int, Dataset]

    class Config:
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def extract_path(cls, values):
        # File name is build from the collector name, the event code, and the pulse ID of the first event
        name = (
            values["collector_name"]
            + "_"
            + str(values["event_code"])
            + "_"
            + str(values["trigger_pulse_id"])
        )
        values.update(name=name)

        # Path is generated from date
        directory = Path(
            values["trigger_date"].strftime("%Y"),
            values["trigger_date"].strftime("%Y-%m-%d"),
        )
        path = directory / f"{name}.h5"
        values.update(path=path)

        values.update(datasets=dict())

        values.update(events=[])

        return values

    async def index(self, indexer_url):
        """Send metadata from all dataset to the indexer"""
        for dataset in self.datasets.values():
            await dataset.index(indexer_url)

    def update(self, event: Event):
        """
        Add an event to the NeXus file
        """
        dataset = self.datasets.get(event.trigger_pulse_id)
        # If the event belongs to a different dataset (different trigger_pulse_id), create a new Dataset
        if dataset is None:
            dataset = Dataset(
                collector_id=self.collector_id,
                trigger_date=event.trigger_date,
                trigger_pulse_id=event.trigger_pulse_id,
                path=self.path,
            )
            self.datasets.update({event.trigger_pulse_id: dataset})

        self.events.append(event)

    def write(self):
        """
        Write NeXus file into storage
        """
        try:
            print(repr(self), f"writing to '{self.path}'")
            absolute_path = settings.storage_path / self.path
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            h5file = File(absolute_path, "w")
            entry = h5file.create_group(name="entry")
            entry.attrs["NX_class"] = "NXentry"

            for event in self.events:
                trigger_key = f"trigger_{event.trigger_pulse_id}"
                pulse_key = f"pulse_{event.pulse_id}"
                if trigger_key not in entry:
                    trigger_group = entry.create_group(name=trigger_key)
                    trigger_group.attrs["trigger_pulse_id"] = event.trigger_pulse_id
                    trigger_group.attrs[
                        "trigger_timestamp"
                    ] = event.trigger_date.isoformat()
                if pulse_key not in entry[trigger_key]:
                    entry[trigger_key].create_group(name=pulse_key)
                    entry[trigger_key][pulse_key].attrs["pulse_id"] = event.pulse_id
                    entry[trigger_key][pulse_key].attrs[
                        "timestamp"
                    ] = event.data_date.isoformat()

                entry[trigger_key][pulse_key][event.pv_name] = event.value

            h5file.close()
            print(repr(self), "writing done.")
        except Exception as e:
            print(repr(self), "writing failed!")
            print(e)

    def __repr__(self):
        return f"Dataset({self.name})"


def write_file(nexus_file: NexusFile):
    """Convenience method to write the NeXus files from a ProcessPoolExecutor"""
    nexus_file.write()
