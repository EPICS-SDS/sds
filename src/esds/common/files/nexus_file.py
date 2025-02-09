import logging
import os.path
from asyncio import Lock
from datetime import datetime
from pathlib import Path
from traceback import print_exc
from typing import Any, Dict, List

import numpy as np
from h5py import File, enum_dtype, special_dtype

from esds.common.files.config import settings
from esds.common.files.dataset import Dataset
from esds.common.files.event import Event

logger = logging.getLogger(__name__)

text_dtype = special_dtype(vlen=str)

p4p_type_to_hdf5 = {
    "?": np.bool_,
    "s": None,
    "b": np.byte,
    "B": np.ubyte,
    "h": np.int16,
    "H": np.uint16,
    "i": np.int32,
    "I": np.uint32,
    "l": np.int64,
    "L": np.uint64,
    "f": np.float32,
    "d": np.float64,
}


class NexusFile:
    """
    Model for a file containing Dataset objects from one or more timing
    event that share the same collector.
    """

    def __init__(
        self,
        collector_id: str,
        collector_name: str,
        parent_path: str,
        file_name: str,
        directory: Path,
    ):
        self.collector_id: str = collector_id
        self.collector_name: str = collector_name
        self.parent_path: str = parent_path
        self.file_name: str = file_name
        self.path: Path = directory / f"{self.file_name}.h5"

        self.datasets: Dict[int, Dataset] = dict()
        self.events: List[Event] = list()
        self.lock: Lock = Lock()

    async def index(self, indexer_url):
        """Send metadata from all dataset to the indexer"""
        for dataset in self.datasets.values():
            await dataset.index(indexer_url)

    def add_event(self, event: Event):
        """
        Add an event to the NeXus file
        """
        dataset = self.datasets.get(event.sds_event_cycle_id)
        # If the event belongs to a different dataset (different sds_event_cycle_id), create a new Dataset
        if dataset is None:
            dataset = Dataset(
                collector_id=self.collector_id,
                sds_event_timestamp=event.sds_event_timestamp,
                sds_cycle_start_timestamp=event.sds_cycle_start_timestamp,
                sds_event_cycle_id=event.sds_event_cycle_id,
                path=self.path,
                beam_info=event.attributes.get("beamInfo", None),
            )
            self.datasets.update({event.sds_event_cycle_id: dataset})

        # Update the timestamp of the start of the cycle when the SDS event happened
        if event.sds_cycle_start_timestamp != datetime(1970, 1, 1):
            dataset.sds_cycle_start_timestamp = event.sds_cycle_start_timestamp

        self.events.append(event)

    def clear_events(self):
        """
        Convenience method to clear the list of events after a partial write to disk.
        """
        self.events.clear()

    def add_dataset(self, dataset: Dataset):
        self.datasets.update({dataset.sds_event_cycle_id: dataset})

    def write_from_events(self):
        """
        Write NeXus file from a list of Event objects (for collector)
        """
        try:
            h5file = self._get_h5file()
        except Exception as e:
            logger.warning(f"{repr(self)} writing event failed! Problem with the file.")
            print_exc()
            logger.warning(e)
            return False

        if h5file is None:
            return False

        try:
            logger.info(f"{repr(self)} writing to '{self.path}'")

            entry = h5file.require_group(name="entry")
            entry.attrs["NX_class"] = "NXentry"
            entry.attrs["collector_name"] = self.collector_name
            entry.attrs["collector_parent_path"] = self.parent_path

            while True:
                # Pop elements from the list
                try:
                    event = self.events.pop(0)
                except IndexError:
                    break
                sds_event_key = f"sds_event_{event.sds_event_cycle_id}"
                cycle_key = f"cycle_{event.cycle_id}"

                sds_event_group = entry.require_group(name=sds_event_key)
                sds_event_group.attrs["NX_class"] = "NXsubentry"
                sds_event_group.attrs["cycle_id"] = event.sds_event_cycle_id
                if event.sds_cycle_start_timestamp != datetime(1970, 1, 1):
                    sds_event_group.attrs[
                        "cycle_start_timestamp"
                    ] = event.sds_cycle_start_timestamp.isoformat()
                sds_event_group.attrs[
                    "timestamp"
                ] = event.sds_event_timestamp.isoformat()
                sds_event_group.attrs["event_code"] = event.timing_event_code

                entry[sds_event_key].require_group(name=cycle_key)
                # Adding attributes about cycle (should be the same for all events)
                cycle_attributes = entry[sds_event_key][cycle_key].attrs
                cycle_attributes["NX_class"] = "NXdata"
                cycle_attributes["cycle_id"] = event.cycle_id
                cycle_attributes["timestamp"] = event.cycle_id_timestamp.isoformat()

                if "beamInfo" in event.attributes:
                    self._recursively_add_attributes(
                        cycle_attributes,
                        event.attributes.pop("beamInfo"),
                        prefix="beamInfo",
                    )

                try:
                    self._parse_value(
                        entry[sds_event_key][cycle_key],
                        event.pv_name,
                        event.value,
                        event.type,
                    )
                except ValueError as e:
                    logger.error(
                        f"Duplicated value for PV {event.pv_name}. SDS event {sds_event_key}. Cycle ID {cycle_key}"
                    )
                    logger.error(e)

                    continue

                # Acq info and event metadata
                acquisition_attributes = entry[sds_event_key][cycle_key][
                    event.pv_name
                ].attrs
                acquisition_attributes["timestamp"] = event.data_timestamp.isoformat()

                self._recursively_add_attributes(
                    acquisition_attributes, event.attributes
                )

            h5file.close()
            logger.info(f"{repr(self)} writing done.")
            return True
        except Exception as e:
            logger.warning(f"{repr(self)} writing event failed!")
            print_exc()
            logger.warning(e)
            h5file.close()
            return False

    def _recursively_add_attributes(
        self, attributes_node, attributes_dict: Dict[str, Any], prefix=None
    ):
        for key, value in attributes_dict.items():
            new_key = key if prefix is None else f"{prefix}.{key}"
            if key == "timeStamp":
                attributes_node[new_key] = datetime.fromtimestamp(
                    value["secondsPastEpoch"] + value["nanoseconds"] * 1e-9
                ).isoformat()
            elif (
                isinstance(value, list)
                or isinstance(value, set)
                or isinstance(value, tuple)
            ):
                for i, val in enumerate(value):
                    self._recursively_add_attributes(
                        attributes_node, val, prefix=f"{new_key}_{i}"
                    )
            elif isinstance(value, dict):
                self._recursively_add_attributes(attributes_node, value, prefix=new_key)
            else:
                attributes_node[new_key] = value

    def _parse_value(self, parent, key, value, t):
        if isinstance(value, dict):
            if t.get_id() == "enum_t":
                enum_type = enum_dtype(
                    {f"{c}": i for i, c in enumerate(value["choices"])}, basetype="i"
                )
                parent.create_dataset(key, data=value["index"], dtype=enum_type)
            else:
                group = parent.create_group(name=key)
                for k, v in value.items():
                    self._parse_value(group, k, v, t.subtypes[k])
        else:
            # Finding data type
            if (
                isinstance(t.type, list)
                or isinstance(t.type, set)
                or isinstance(t.type, tuple)
            ):
                # For union types, infer type from data
                dtype = np.array(value).dtype
            else:
                # t.type[0] = 'a' for arrays
                dtype = p4p_type_to_hdf5.get(t.type[-1], None)
            if settings.compression and isinstance(value, np.ndarray):
                # Only compress arrays
                parent.create_dataset(
                    key,
                    data=value,
                    compression="lzf",
                    dtype=dtype,
                )
            else:
                parent.create_dataset(key, data=value, dtype=dtype)

    def write_from_datasets(self, include_pvs=None):
        """
        Write a combined NeXus file from a list of files (for retriever)
        """
        try:
            h5file = self._get_h5file()
            if h5file is None:
                return

            logger.info(f"{repr(self)} writing to '{self.path}'")
            entry = h5file.require_group(name="entry")
            entry.attrs["NX_class"] = "NXentry"
            entry.attrs["collector_name"] = self.collector_name
            entry.attrs["collector_parent_path"] = self.parent_path

            for dataset in self.datasets.values():
                origin = File(settings.storage_path / dataset.path, "r")
                entry_origin = origin["entry"]
                sds_event_origin = entry_origin[
                    f"sds_event_{dataset.sds_event_cycle_id}"
                ]
                if include_pvs is None:
                    h5file.copy(sds_event_origin, entry)
                else:
                    sds_event = entry.require_group(
                        f"sds_event_{dataset.sds_event_cycle_id}"
                    )

                    for att in sds_event_origin.attrs:
                        sds_event.attrs[att] = sds_event_origin.attrs[att]
                    for cycle_id in sds_event_origin:
                        cycle = sds_event.require_group(cycle_id)
                        cycle_origin = sds_event_origin[cycle_id]
                        for att in cycle_origin.attrs:
                            cycle.attrs[att] = cycle_origin.attrs[att]
                        for pv in cycle_origin:
                            if pv in include_pvs:
                                h5file.copy(cycle_origin[pv], cycle)
                origin.close()

            h5file.close()
            logger.info(f"{repr(self)} writing done.")
        except Exception as e:
            logger.warning(f"{repr(self)} writing failed!")
            logger.warning(e)

    def _get_h5file(self):
        absolute_path = settings.storage_path / self.path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        if os.path.exists(absolute_path):
            return File(absolute_path, "a")
        else:
            return File(absolute_path, "w")

    def __repr__(self):
        return f"Dataset({self.file_name})"

    def getsize(self) -> float:
        return os.path.getsize(settings.storage_path / self.path)


def write_to_file(nexus_file: NexusFile):
    """Convenience method to write the NeXus files from a ProcessPoolExecutor"""
    nexus_file.write_from_events()
