from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SdsPVValue(BaseModel):
    """Simpler representation of a tuple of PV timestamp and value"""

    pv_ts: float
    pv_value: Any

    def __str__(self) -> str:
        return f"{datetime.fromtimestamp(self.pv_ts).time()}\t{self.pv_value}"


class SdsPVBase(SdsPVValue):
    """PV with more metadata related to events"""

    pv_type: str
    pv_name: str
    start_event_cycle_id: int
    start_event_ts: float
    main_event_cycle_id: int
    main_event_ts: float
    acq_event_name: str
    acq_event_code: int
    acq_event_delay: float

    # Beam info from databuffer
    beam_mode: str
    beam_state: str
    beam_present: str
    beam_len: float
    beam_energy: float
    beam_dest: str
    beam_curr: float


class SdsPV(SdsPVBase):
    """PV with SDS metadata"""

    sds_evt_code: int
    sds_ts: float
    sds_cycle_id: float
