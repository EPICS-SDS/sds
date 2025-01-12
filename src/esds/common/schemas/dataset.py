from datetime import UTC, datetime
from pathlib import PurePosixPath as UnvalidatedPurePosixPath
from typing import Any, Dict, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
)
from pydantic_core import CoreSchema, core_schema


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


class DataseDefinition(BaseModel):
    collector_id: str
    sds_event_timestamp: datetime
    sds_cycle_start_timestamp: datetime
    sds_event_cycle_id: int
    path: PurePosixPath


class DatasetBase(DataseDefinition):
    beam_info: Dict[str, Any]


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
