from typing import Any, Optional

from datetime import datetime
from pydantic import BaseModel, Field
from pathlib import PurePosixPath

from pydantic.validators import _VALIDATORS


def validate_pure_posix_path(v: Any) -> PurePosixPath:
    """Attempt to convert a value to a PurePosixPath"""
    return PurePosixPath(v)


_VALIDATORS.append((PurePosixPath, [validate_pure_posix_path]))


class DatasetBase(BaseModel):
    collector_id: str
    trigger_pulse_id: int
    path: PurePosixPath


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
