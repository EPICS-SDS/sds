from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Event(BaseModel):
    """
    Data corresponding to an updated value event for one of the monitored PVs.
    """

    pv_name: str
    value: Any
    type: Any
    timing_event_code: int
    data_timestamp: datetime
    cycle_id_timestamp: datetime
    sds_event_timestamp: datetime
    sds_cycle_start_timestamp: datetime = datetime(1970, 1, 1)
    cycle_id: int
    sds_event_cycle_id: int
    event_size: float

    attributes: Optional[Dict[str, Any]]
