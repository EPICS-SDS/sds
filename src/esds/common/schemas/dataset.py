from typing import Any, Dict, Optional

from datetime import datetime
from pydantic import BaseModel, GetJsonSchemaHandler, Field
from pydantic_core import CoreSchema
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
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> Dict[str, Any]:
        json_schema = super().__get_pydantic_json_schema__(core_schema, handler)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema.update(type="string", example="/directory/file.h5")
        return json_schema


class AcqInfo(BaseModel):
    type: str
    id: int


class AcqEvent(BaseModel):
    name: str
    evr: str
    delay: float
    code: int
    timestamp: datetime


class BeamInfo(BaseModel):
    mode: str
    state: str
    present: str
    len: float
    energy: float
    dest: str
    curr: float


class DataseDefinition(BaseModel):
    collector_id: str
    sds_event_timestamp: datetime
    sds_event_pulse_id: int
    path: PurePosixPath


class DatasetBase(DataseDefinition):
    beam_info: BeamInfo


class DatasetCreate(DatasetBase):
    expire_in: Optional[int]
    timestamp: Optional[datetime] = Field(
        default_factory=datetime.utcnow, alias="@timestamp"
    )


class DatasetInDBBase(DatasetBase):
    id: str

    class Config:
        orm_mode = True


class Dataset(DatasetInDBBase):
    pass
