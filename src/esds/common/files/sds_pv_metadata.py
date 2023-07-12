from datetime import datetime

from pydantic import BaseModel


class AcqEvent(BaseModel):
    timestamp: datetime
    name: str
    delay: float
    code: int
    evr: str


class BeamInfo(BaseModel):
    mode: str
    state: str
    present: str
    len: float
    energy: float
    dest: str
    curr: float


class ArrayInfo(BaseModel):
    tick: float
    size: int


class BufferInfo(BaseModel):
    size: int
    idx: int
