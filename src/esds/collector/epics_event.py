from datetime import datetime
from typing import Any, Dict

from p4p import Value
from pydantic import root_validator

from esds.common.files import AcqEvent, ArrayInfo, BeamInfo, BufferInfo, Event
from esds.common.p4p_type import P4pType


class EpicsEvent(Event):
    @root_validator(pre=True)
    def extract_values(cls, values: Dict[str, Any]):
        if "value" not in values:
            return values

        value: Value = values["value"]
        # value
        values.update(
            value=value.todict("value"),
            type=P4pType(value.type()["value"]),
            data_timestamp=datetime.fromtimestamp(
                value.timeStamp.secondsPastEpoch + value.timeStamp.nanoseconds * 1e-9
            ),
        )

        # pulse_id
        pulse_id = value.get("pulseId")
        if pulse_id is not None:
            values.update(
                pulse_id=pulse_id.value,
                pulse_id_timestamp=datetime.fromtimestamp(
                    pulse_id.timeStamp.secondsPastEpoch
                    + pulse_id.timeStamp.nanoseconds * 1e-9
                ),
            )

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

            # buffer info (for circular buffer)
            buffer_info = sds_info.get("buffer")
            if buffer_info is not None:
                values.update(
                    buffer_info=BufferInfo(
                        size=buffer_info.size,
                        idx=buffer_info.idx,
                    )
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
        # acqEvt
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
        # arrayInfo
        array_info = value.get("arrayInfo")
        if array_info is not None:
            values.update(
                array_info=ArrayInfo(
                    tick=array_info.tick,
                    size=array_info.size,
                )
            )

        return values
