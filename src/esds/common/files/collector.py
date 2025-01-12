from typing import List, Optional, Set

from pydantic import BaseModel, ConfigDict, RootModel


class CollectorDefinition(BaseModel):
    """
    Model for Collector information as stored in the collector definitions file.
    """

    name: str
    pvs: Set[str]
    event_code: int
    parent_path: str
    collector_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class CollectorList(RootModel):
    root: List[CollectorDefinition]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    def __len__(self):
        return len(self.root)
