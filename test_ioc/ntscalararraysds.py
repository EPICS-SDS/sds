#!/usr/bin/env python
# -*- coding: utf-8 -*-


from p4p.nt import NTScalar
from p4p.nt.scalar import _metaHelper
from p4p.nt.common import alarm, timeStamp
from p4p.wrapper import Type


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

    def wrap(self, value):
        if type(value) is dict:
            wrapped_value = super().wrap(value["value"])

            event_timestamp = timeStamp()
            event_timestamp["secondsPastEpoch"] = value["timestamp"] // 1e9
            event_timestamp["nanoseconds"] = value["timestamp"] % 1e9
            wrapped_value["timeStamp"] = event_timestamp

            trigger_timestamp = timeStamp()
            trigger_timestamp["secondsPastEpoch"] = value["trigger_timestamp"] // 1e9
            trigger_timestamp["nanoseconds"] = value["trigger_timestamp"] % 1e9

            wrapped_value["sdsInfo"]["pulseId"] = value["pulse_id"]
            wrapped_value["sdsInfo"]["evtCode"] = value["event_code"]
            wrapped_value["sdsInfo"]["timeStamp"] = trigger_timestamp

            wrapped_value["pulseId"]["value"] = value["pulse_id"]
            wrapped_value["pulseId"]["timeStamp"] = trigger_timestamp

        else:
            wrapped_value = super().wrap(value)
        return wrapped_value
