from datetime import datetime, UTC
from typing import Set

from pydantic import BaseModel, ConfigDict, Field


class CollectorDefinition(BaseModel):
    name: str
    event_name: str
    event_code: int
    pvs: Set[str]


class CollectorBase(CollectorDefinition):
    host: str


class CollectorCreate(CollectorBase):
    created: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CollectorInDBBase(CollectorBase):
    id: str
    created: datetime

    model_config = ConfigDict(from_attributes=True)


class Collector(CollectorInDBBase):
    pass
