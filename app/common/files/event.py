from datetime import datetime
from typing import Any

from common.files import BeamInfo
from pydantic import BaseModel


class Event(BaseModel):
    """
    Data corresponding to an updated value event for one of the monitored PVs.
    """

    pv_name: str
    value: Any
    timing_event_code: int
    data_timestamp: datetime
    trigger_timestamp: datetime
    pulse_id: int
    trigger_pulse_id: int

    beam_info: BeamInfo
