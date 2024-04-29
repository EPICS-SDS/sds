from typing import List, Set

from pydantic import BaseModel


class CollectorDefinition(BaseModel):
    """
    Model for Collector information as stored in the collector definitions file.
    """

    name: str
    pvs: Set[str]
    event_name: str
    event_code: int


CollectorList = List[CollectorDefinition]
