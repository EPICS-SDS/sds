import base64
import uuid
from datetime import UTC, datetime
from typing import List, Set

from pydantic import BaseModel, ConfigDict, Field


def generate_base64_uuid():
    """
    Generates an ID for the collectors using a similar algorithm as elastic does
    """
    # Generate a random UUID
    random_uuid = uuid.uuid4()
    # Convert UUID to bytes, encode in Base64, and decode to a string
    base64_id = base64.urlsafe_b64encode(random_uuid.bytes).rstrip(b"=").decode("utf-8")
    return base64_id


class CollectorDefinition(BaseModel):
    name: str
    event_code: int
    pvs: Set[str]
    parent_path: str = "/"
    collector_id: str = Field(default_factory=generate_base64_uuid)


class CollectorBase(CollectorDefinition):
    version: int = 1


class CollectorCreate(CollectorBase):
    created: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CollectorInDBBase(CollectorBase):
    created: datetime

    model_config = ConfigDict(from_attributes=True)


class Collector(CollectorInDBBase):
    pass


class CollectorIdList(BaseModel):
    collector_id: List[str]
