from typing import List, Set

from pydantic import BaseModel, ConfigDict, RootModel


class CollectorDefinition(BaseModel):
    """
    Model for Collector information as stored in the collector definitions file.
    """

    name: str
    pvs: Set[str]
    event_name: str
    event_code: int

    model_config = ConfigDict(from_attributes=True)


class CollectorList(RootModel):
    root: List[CollectorDefinition]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    def __len__(self):
        return len(self.root)
