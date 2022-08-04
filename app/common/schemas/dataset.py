from typing import Any, Optional

from datetime import datetime
from pydantic import BaseModel
from pathlib import PurePosixPath as UnvalidatedPurePosixPath


class PurePosixPath(UnvalidatedPurePosixPath):
    """
    Subclassing PurePosixPath to add Pydantic validator
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: Any):
        if isinstance(v, str):
            return UnvalidatedPurePosixPath(v)
        elif isinstance(v, UnvalidatedPurePosixPath):
            return v
        raise TypeError("Type must be string or PurePosixPath")

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string", example="/directory/file.h5")


class DatasetBase(BaseModel):
    collector_id: str
    trigger_date: datetime
    trigger_pulse_id: int
    path: PurePosixPath


class DatasetCreate(DatasetBase):
    expire_in: Optional[int]


class DatasetInDBBase(DatasetBase):
    id: str

    class Config:
        orm_mode = True


class Dataset(DatasetInDBBase):
    pass
