#!/usr/bin/env python
# -*- coding: utf-8 -*-


from p4p.nt import NTScalar
from p4p.nt.common import alarm, timeStamp
from p4p.nt.scalar import _metaHelper
from p4p.wrapper import Type

from sds_pv import SdsPV


class NTScalarArraySDS(NTScalar):
    @staticmethod
    def buildType(valtype, extra=[], display=False, control=False, value_alarm=False):
        """Build type"""
        F = [
            ("value", valtype),
            ("alarm", alarm),
            ("timeStamp", timeStamp),
        ]
        F.extend(
            [
                (
                    "beamInfo",
                    (
                        "S",
                        None,
                        [
                            ("mode", "s"),
                            ("state", "s"),
                            ("present", "s"),
                            ("len", "d"),
                            ("energy", "d"),
                            ("dest", "s"),
                            ("curr", "d"),
                        ],
                    ),
                )
            ]
        )
        F.extend(
            [
                (
                    "sdsInfo",
                    (
                        "S",
                        None,
                        [
                            ("pulseId", "l"),
                            ("id", "d"),
                            ("evtCode", "i"),
                            ("alarm", alarm),
                            ("timeStamp", timeStamp),
                        ],
                    ),
                )
            ]
        )
        F.extend(
            [
                (
                    "pulseId",
                    (
                        "S",
                        None,
                        [
                            ("value", "l"),
                            ("alarm", alarm),
                            ("timeStamp", timeStamp),
                        ],
                    ),
                )
            ]
        )
        F.extend(
            [
                (
                    "acqEvt",
                    (
                        "S",
                        None,
                        [
                            ("name", "s"),
                            ("evr", "s"),
                            ("delay", "d"),
                            ("code", "d"),
                            ("alarm", alarm),
                            ("timeStamp", timeStamp),
                        ],
                    ),
                )
            ]
        )
        _metaHelper(
            F, valtype, display=display, control=control, valueAlarm=value_alarm
        )
        F.extend(extra)
        return Type(id="epics:nt/NTScalarArray:1.0", spec=F)

    def wrap(self, sds_pv: SdsPV):
        if type(sds_pv) is SdsPV:
            wrapped_value = super().wrap(sds_pv.pv_value[:2])

            # Copy timestamp from PV
            pv_timestamp = timeStamp()
            pv_timestamp["secondsPastEpoch"] = sds_pv.pv_ts // 1e9
            pv_timestamp["nanoseconds"] = sds_pv.pv_ts % 1e9
            wrapped_value["timeStamp"] = pv_timestamp

            # Pulse ID information comes from the 14 Hz event
            mainevent_timestamp = timeStamp()
            mainevent_timestamp["secondsPastEpoch"] = sds_pv.pv_ts // 1e9
            mainevent_timestamp["nanoseconds"] = sds_pv.pv_ts % 1e9

            wrapped_value["pulseId"]["value"] = sds_pv.main_event_pulse_id
            wrapped_value["pulseId"]["timeStamp"] = mainevent_timestamp

            # Acq info comes from start acq. event
            start_event_ts = timeStamp()
            start_event_ts["secondsPastEpoch"] = sds_pv.main_event_pulse_id // 1e9
            start_event_ts["nanoseconds"] = sds_pv.start_event_ts % 1e9

            wrapped_value["acqEvt"]["timeStamp"] = start_event_ts
            wrapped_value["acqEvt"]["delay"] = sds_pv.acq_event_delay
            wrapped_value["acqEvt"]["code"] = sds_pv.acq_event_code
            wrapped_value["acqEvt"]["name"] = sds_pv.acq_event_name

            # SDS info
            sds_timestamp = timeStamp()
            sds_timestamp["secondsPastEpoch"] = sds_pv.sds_ts // 1e9
            sds_timestamp["nanoseconds"] = sds_pv.sds_ts % 1e9

            wrapped_value["sdsInfo"][
                "pulseId"
            ] = sds_pv.main_event_pulse_id  # long int (64b)
            wrapped_value["sdsInfo"]["evtCode"] = sds_pv.sds_evt_code
            wrapped_value["sdsInfo"]["timeStamp"] = sds_timestamp

            # Beam info
            wrapped_value["beamInfo"]["mode"] = sds_pv.beam_mode
            wrapped_value["beamInfo"]["state"] = sds_pv.beam_state
            wrapped_value["beamInfo"]["present"] = sds_pv.beam_present
            wrapped_value["beamInfo"]["len"] = sds_pv.beam_len
            wrapped_value["beamInfo"]["energy"] = sds_pv.beam_energy
            wrapped_value["beamInfo"]["dest"] = sds_pv.beam_dest
            wrapped_value["beamInfo"]["curr"] = sds_pv.beam_curr
        else:
            wrapped_value = super().wrap(sds_pv)
        return wrapped_value
