from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from esds.common.files import AcqEvent, ArrayInfo, BeamInfo, BufferInfo


class Event(BaseModel):
    """
    Data corresponding to an updated value event for one of the monitored PVs.
    """

    pv_name: str
    value: Any
    type: Any
    timing_event_code: int
    data_timestamp: datetime
    pulse_id_timestamp: datetime
    sds_event_timestamp: datetime
    pulse_id: int
    sds_event_pulse_id: int

    acq_event: AcqEvent
    beam_info: BeamInfo

    buffer_info: Optional[BufferInfo] = None

    array_info: Optional[ArrayInfo] = None
