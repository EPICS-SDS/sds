from datetime import datetime
from typing import Any

from pydantic import BaseModel
from sds.common.files import AcqEvent, AcqInfo, BeamInfo


class Event(BaseModel):
    """
    Data corresponding to an updated value event for one of the monitored PVs.
    """

    pv_name: str
    value: Any
    timing_event_code: int
    data_timestamp: datetime
    sds_event_timestamp: datetime
    pulse_id: int
    sds_event_pulse_id: int

    acq_info: AcqInfo
    acq_event: AcqEvent
    beam_info: BeamInfo
