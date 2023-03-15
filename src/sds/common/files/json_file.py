from traceback import print_exc
from typing import Dict, List
from h5py import File, Group
from sds.common.files.config import settings
from sds.common.files.dataset import Dataset

import numpy as np


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
        self.datasets.update({dataset.sds_event_pulse_id: dataset})

    def json(self):
        """
        Write a combined Json file from a list of files (for retriever)
        """
        try:
            json_dict = {}

            for dataset in self.datasets.values():
                origin = File(settings.storage_path / dataset.path, "r")

                json_dict[f"sds_event_{dataset.sds_event_pulse_id}"] = {}
                for pulse in origin["entry"][f"sds_event_{dataset.sds_event_pulse_id}"]:
                    json_dict[f"sds_event_{dataset.sds_event_pulse_id}"][pulse] = {}
                    for pv in origin["entry"][
                        f"sds_event_{dataset.sds_event_pulse_id}"
                    ][pulse]:
                        if self.pvs is None or pv in self.pvs:
                            json_dict[f"sds_event_{dataset.sds_event_pulse_id}"][pulse][
                                pv
                            ] = self.todict(
                                origin["entry"][
                                    f"sds_event_{dataset.sds_event_pulse_id}"
                                ][pulse][pv]
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
    np.ndarray: lambda arr: arr.tolist(),
}
