from traceback import print_exc
from typing import Dict, List

import numpy as np
from h5py import File, Group

from esds.common.files.config import settings
from esds.common.files.dataset import Dataset

numpy_encoder = {
    np.integer: int,
    np.floating: float,
    np.ndarray: lambda arr: arr.tolist(),
}


class JsonFile:
    """
    Model for a JSON file containing Dataset objects from one or more timing
    event that share the same collector.
    """

    def __init__(
        self,
        pvs: List[str] = None,
    ):
        self.pvs = pvs

        self.datasets: Dict[int, Dataset] = dict()

    def add_dataset(self, dataset: Dataset):
        self.datasets.update({dataset.sds_event_cycle_id: dataset})

    def json(self):
        """
        Write a combined Json file from a list of files (for retriever)
        """
        try:
            json_dict = {"attrs": {}, "events": {}}

            for dataset in self.datasets.values():
                origin = File(settings.storage_path / dataset.path, "r")

                for att in origin["entry"].attrs:
                    # Remove nexus tags
                    if not att.startswith("NX"):
                        json_dict["attrs"][att] = origin["entry"].attrs[att]

                json_event = json_dict["events"][
                    f"sds_event_{dataset.sds_event_cycle_id}"
                ] = {"attrs": {}, "cycles": {}}
                origin_event = origin["entry"][
                    f"sds_event_{dataset.sds_event_cycle_id}"
                ]
                for att in origin_event.attrs:
                    # Remove nexus tags
                    if not att.startswith("NX"):
                        json_event["attrs"][att] = origin_event.attrs[att]
                for cycle in origin_event:
                    origin_cycle = origin_event[cycle]
                    json_cycle = json_event["cycles"][cycle] = {"attrs": {}, "pvs": {}}

                    for att in origin_cycle.attrs:
                        # Remove nexus tags
                        if not att.startswith("NX"):
                            json_cycle["attrs"][att] = origin_cycle.attrs[att]

                    for pv in origin_cycle:
                        if self.pvs is None or pv in self.pvs:
                            json_cycle["pvs"][pv] = {"attrs": {}, "values": {}}

                            for att in origin_cycle[pv].attrs:
                                # Remove nexus tags
                                if not att.startswith("NX"):
                                    json_cycle["pvs"][pv]["attrs"][att] = origin_cycle[
                                        pv
                                    ].attrs[att]

                            json_cycle["pvs"][pv]["values"] = self.todict(
                                origin_cycle[pv]
                            )

            return json_dict
        except Exception:
            print(repr(self), "writing failed!")
            print_exc()

    def todict(self, value):
        if isinstance(value, Group):
            new_dict = {}
            for key in value.keys():
                new_dict[key] = self.todict(value[key])
            return new_dict
        else:
            return value[:]


numpy_encoder = {
    np.integer: int,
    np.floating: float,
    np.bool_: bool,
    np.ndarray: lambda arr: arr.tolist(),
}
