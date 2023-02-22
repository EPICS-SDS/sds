from datetime import datetime
from typing import Any, Dict

from p4p import Value
from pydantic import root_validator
from common.files import AcqEvent, AcqInfo, BeamInfo
from common.files import Event


class EpicsEvent(Event):
    @root_validator(pre=True)
    def extract_values(cls, values: Dict[str, Any]):
        if "value" not in values:
            return values

        value: Value = values["value"]
        # value
        values.update(
            value=value.get('value').todict(),
            data_timestamp=datetime.fromtimestamp(
                value.timeStamp.secondsPastEpoch
                + value.timeStamp.nanoseconds * 1e-9
            ),
        )

        # pulse_id
        pulse_id = value.get("pulseId")
        if pulse_id is not None:
            values.update(pulse_id=pulse_id.value)

        # eventCode
        sds_info = value.get("sdsInfo")
        if sds_info is not None:
            values.update(
                sds_event_timestamp=datetime.fromtimestamp(
                    sds_info.timeStamp.secondsPastEpoch
                    + sds_info.timeStamp.nanoseconds * 1e-9
                ),
                sds_event_pulse_id=sds_info.pulseId,
                timing_event_code=int(sds_info.evtCode),
            )

        # beamInfo
        beam_info = value.get("beamInfo")
        if beam_info is not None:
            values.update(
                beam_info=BeamInfo(
                    mode=beam_info.mode,
                    state=beam_info.state,
                    present=beam_info.present,
                    len=beam_info.len,
                    energy=beam_info.energy,
                    dest=beam_info.dest,
                    curr=beam_info.curr,
                )
            )
        # acqInfo
        acq_info = value.get("acqInfo")
        if acq_info is not None:
            values.update(
                acq_info=AcqInfo(
                    acq_type=acq_info["type"],
                    id=acq_info.id,
                )
            )
        # acqInfo
        acq_event = value.get("acqEvt")
        if acq_event is not None:
            values.update(
                acq_event=AcqEvent(
                    timestamp=datetime.fromtimestamp(
                        acq_event.timeStamp.secondsPastEpoch
                        + acq_event.timeStamp.nanoseconds * 1e-9
                    ),
                    name=acq_event.name,
                    delay=acq_event.delay,
                    code=acq_event.code,
                    evr=acq_event.evr,
                )
            )

        return values
