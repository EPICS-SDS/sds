from pydantic import BaseModel


class BeamInfo(BaseModel):
    mode: str
    state: str
    present: str
    len: float
    energy: float
    dest: str
    curr: float
