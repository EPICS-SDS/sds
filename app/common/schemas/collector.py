from datetime import datetime
from typing import List, Set

from pydantic import BaseModel, Field


class CollectorBase(BaseModel):
    name: str
    event_name: str
    event_code: int
    pvs: Set[str]


class CollectorCreate(CollectorBase):
    created: datetime = Field(default_factory=datetime.utcnow)


class CollectorInDBBase(CollectorBase):
    id: str
    created: datetime

    class Config:
        orm_mode = True


class Collector(CollectorInDBBase):
    pass


class CollectorList(BaseModel):
    __root__: List[CollectorBase]
