from datetime import datetime
from typing import Any

from pydantic import BaseModel


class Event(BaseModel):
    """
    Data corresponding to an updated value event for one of the monitored PVs.
    """

    pv_name: str
    value: Any
    timing_event_name: str
    timing_event_code: int
    data_date: datetime
    trigger_date: datetime
    pulse_id: int
    trigger_pulse_id: int
