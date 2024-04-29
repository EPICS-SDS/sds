from typing import Any, Dict, Optional

from datetime import datetime, UTC
from pydantic import (
    BaseModel,
    ConfigDict,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
    Field,
)
from pydantic_core import CoreSchema, core_schema
from pathlib import PurePosixPath as UnvalidatedPurePosixPath


class PurePosixPath(UnvalidatedPurePosixPath):
    """
    Subclassing PurePosixPath to add Pydantic validator
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def validate2(cls, v):
        if isinstance(v, str):
            return UnvalidatedPurePosixPath(v)
        elif isinstance(v, UnvalidatedPurePosixPath):
            return v
        raise TypeError("Type must be string or PurePosixPath")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls.validate2, handler(UnvalidatedPurePosixPath)
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> Dict[str, Any]:
        json_schema = json_schema = handler(core_schema)
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
    expire_in: Optional[int] = None
    timestamp: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(tz=UTC), alias="@timestamp"
    )


class DatasetInDBBase(DatasetBase):
    id: str

    model_config = ConfigDict(from_attributes=True)


class Dataset(DatasetInDBBase):
    pass
