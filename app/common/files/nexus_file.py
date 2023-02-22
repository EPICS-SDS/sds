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
        dataset = self.datasets.get(event.sds_event_pulse_id)
        # If the event belongs to a different dataset (different sds_event_pulse_id), create a new Dataset
        if dataset is None:
            dataset = Dataset(
                collector_id=self.collector_id,
                sds_event_timestamp=event.sds_event_timestamp,
                sds_event_pulse_id=event.sds_event_pulse_id,
                path=self.path,
                beam_info=event.beam_info,
            )
            self.datasets.update({event.sds_event_pulse_id: dataset})

        self.events.append(event)

    def add_dataset(self, dataset: Dataset):
        self.datasets.update({dataset.sds_event_pulse_id: dataset})

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
                sds_event_key = f"sds_event_{event.sds_event_pulse_id}"
                pulse_key = f"pulse_{event.pulse_id}"
                if sds_event_key not in entry:
                    sds_event_group = entry.create_group(name=sds_event_key)
                    sds_event_group.attrs["pulse_id"] = event.sds_event_pulse_id
                    sds_event_group.attrs[
                        "timestamp"
                    ] = event.sds_event_timestamp.isoformat()
                    sds_event_group.attrs["event_code"] = event.timing_event_code
                if pulse_key not in entry[sds_event_key]:
                    entry[sds_event_key].create_group(name=pulse_key)
                    # Adding attributes about pulse (should be the same for all events)
                    pulse_attributes = entry[sds_event_key][pulse_key].attrs
                    pulse_attributes["pulse_id"] = event.pulse_id
                    pulse_attributes["timestamp"] = event.data_timestamp.isoformat()
                    pulse_attributes["beam_info.curr"] = event.beam_info.curr
                    pulse_attributes["beam_info.dest"] = event.beam_info.dest
                    pulse_attributes["beam_info.energy"] = event.beam_info.energy
                    pulse_attributes["beam_info.len"] = event.beam_info.len
                    pulse_attributes["beam_info.mode"] = event.beam_info.mode
                    pulse_attributes["beam_info.present"] = event.beam_info.present
                    pulse_attributes["beam_info.state"] = event.beam_info.state

                entry[sds_event_key][pulse_key][event.pv_name] = event.value
                # Acq info and event metadata
                acquisition_attributes = entry[sds_event_key][pulse_key][
                    event.pv_name
                ].attrs
                acquisition_attributes[
                    "acq_event.timestamp"
                ] = event.acq_event.timestamp.isoformat()
                acquisition_attributes["acq_info.type"] = event.acq_info.acq_type
                acquisition_attributes["acq_info.id"] = event.acq_info.id
                acquisition_attributes["acq_event.name"] = event.acq_event.name
                acquisition_attributes["acq_event.delay"] = event.acq_event.delay
                acquisition_attributes["acq_event.code"] = event.acq_event.code
                acquisition_attributes["acq_event.evr"] = event.acq_event.evr

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
                data = origin["entry"][f"sds_event_{dataset.sds_event_pulse_id}"]

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
