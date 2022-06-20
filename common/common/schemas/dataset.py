from typing import Optional

from datetime import datetime
from pydantic import BaseModel, Field
from pathlib import Path


class DatasetBase(BaseModel):
    collector_id: str
    trigger_pulse_id: int
    path: Path


class DatasetCreate(DatasetBase):
    expire_in: Optional[int]
    created: datetime = Field(default_factory=datetime.utcnow)


class DatasetInDBBase(DatasetBase):
    id: str
    created: datetime

    class Config:
        orm_mode = True


class Dataset(DatasetInDBBase):
    pass
