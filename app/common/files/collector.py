import os
from pathlib import Path

import nexusformat.nexus.tree as nx
from common.files.dataset import Dataset
from pydantic import BaseModel

from collector.config import settings


class Collector(BaseModel):
    """
    Model for a file containing Dataset objects that belong to the same collector.
    """

    path: Path
    entry: nx.NXentry

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def create(cls, dataset: Dataset):
        collector = cls.construct()
        collector.update(dataset=dataset, create=True)

        return collector

    def update(self, dataset: Dataset, create: bool = False):
        nexus_file = nx.load(settings.storage_path / dataset.path)
        entry_in = nexus_file.entries["entry"]

        if create:
            self.entry = nx.NXentry(attrs=entry_in.attrs)
            self.path = entry_in.attrs["collector_name"] + ".h5"

        data_in = entry_in[f"trigger_{dataset.trigger_pulse_id}"]
        self.entry.insert(data_in)

    async def write(self, directory: str):
        try:
            absolute_path = os.path.join(directory, self.path)
            self.entry.save(absolute_path, mode="w")
        except Exception:
            print(repr(self), "writing failed!")

    def __repr__(self):
        return f"Collector({self.path})"
