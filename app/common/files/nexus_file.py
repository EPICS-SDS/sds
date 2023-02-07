from datetime import datetime
from pathlib import Path
from typing import Dict, List

from common.files.config import settings
from common.files.dataset import Dataset
from common.files.event import Event
from h5py import File


class NexusFile:
    """
    Model for a file containing Dataset objects from one or more timing
    event that share the same collector.
    """

    def __init__(
        self,
        collector_id: str,
        collector_name: str,
        file_name: str,
        directory: Path,
    ):
        self.collector_id: str = collector_id
        self.collector_name: str = collector_name
        self.file_name: str = file_name
        self.path: Path = directory / f"{self.file_name}.h5"

        self.datasets: Dict[int, Dataset] = dict()
        self.events: List[Event] = list()

    async def index(self, indexer_url):
        """Send metadata from all dataset to the indexer"""
        for dataset in self.datasets.values():
            await dataset.index(indexer_url)

    def add_event(self, event: Event):
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

    def add_dataset(self, dataset: Dataset):
        self.datasets.update({dataset.trigger_pulse_id: dataset})

    def write_from_events(self):
        """
        Write NeXus file from a list of Event objects (for collector)
        """
        try:
            print(repr(self), f"writing to '{self.path}'")
            absolute_path = settings.storage_path / self.path
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            h5file = File(absolute_path, "w")
            entry = h5file.create_group(name="entry")
            entry.attrs["NX_class"] = "NXentry"
            entry.attrs["collector_name"] = self.collector_name

            while True:
                # Pop elements from the list
                try:
                    event = self.events.pop(0)
                except IndexError:
                    break
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

    def write_from_datasets(self):
        """
        Write a combined NeXus file from a list of files (for retriever)
        """
        try:
            print(repr(self), f"writing to '{self.path}'")
            h5file = File(self.path, "w")
            entry = h5file.create_group(name="entry")
            entry.attrs["NX_class"] = "NXentry"
            entry.attrs["collector_name"] = self.collector_name

            for dataset in self.datasets.values():
                origin = File(dataset.path, "r")
                data = origin["entry"][f"trigger_{dataset.trigger_pulse_id}"]

                h5file.copy(data, entry)

            h5file.close()
            print(repr(self), "writing done.")
        except Exception as e:
            print(repr(self), "writing failed!")
            print(e)

    def __repr__(self):
        return f"Dataset({self.file_name})"


def write_file(nexus_file: NexusFile):
    """Convenience method to write the NeXus files from a ProcessPoolExecutor"""
    nexus_file.write_from_events()
