#!/usr/bin/env python
# -*- coding: utf-8 -*-


from p4p.nt import NTNDArray
from p4p.nt.common import alarm, timeStamp
from p4p.wrapper import Type


class NTNDArrayWithEvent(NTNDArray):
    @staticmethod
    def buildType(extra=[]):
        """Build type"""
        return Type(
            [
                (
                    "value",
                    (
                        "U",
                        None,
                        [
                            ("booleanValue", "a?"),
                            ("byteValue", "ab"),
                            ("shortValue", "ah"),
                            ("intValue", "ai"),
                            ("longValue", "al"),
                            ("ubyteValue", "aB"),
                            ("ushortValue", "aH"),
                            ("uintValue", "aI"),
                            ("ulongValue", "aL"),
                            ("floatValue", "af"),
                            ("doubleValue", "ad"),
                        ],
                    ),
                ),
                (
                    "codec",
                    (
                        "S",
                        "codec_t",
                        [
                            ("name", "s"),
                            ("parameters", "v"),
                        ],
                    ),
                ),
                ("compressedSize", "l"),
                ("uncompressedSize", "l"),
                ("uniqueId", "i"),
                ("dataTimeStamp", timeStamp),
                ("alarm", alarm),
                ("timeStamp", timeStamp),
                (
                    "dimension",
                    (
                        "aS",
                        "dimension_t",
                        [
                            ("size", "i"),
                            ("offset", "i"),
                            ("fullSize", "i"),
                            ("binning", "i"),
                            ("reverse", "?"),
                        ],
                    ),
                ),
                (
                    "attribute",
                    (
                        "aS",
                        "epics:nt/NTAttribute:1.0",
                        [
                            ("name", "s"),
                            ("value", "v"),
                            ("tags", "as"),
                            ("descriptor", "s"),
                            ("alarm", alarm),
                            ("timestamp", timeStamp),
                            ("sourceType", "i"),
                            ("source", "s"),
                        ],
                    ),
                ),
            ],
            id="epics:nt/NTNDArray:1.0",
        )

    def wrap(self, value):
        if type(value) is dict:
            wrapped_value = super().wrap(value["value"])
            wrapped_value["timeStamp.userTag"] = value["pulse_id"]
            wrapped_value["attribute"] = [
                {
                    "name": "eventName",
                    "value": value["event_name"],
                    "timestamp": wrapped_value["timeStamp"],
                },
                {
                    "name": "eventCode",
                    "value": value["event_code"],
                    "timestamp": wrapped_value["timeStamp"],
                },
            ]
        else:
            wrapped_value = super().wrap(value)
            wrapped_value["timeStamp.userTag"] = 0
        return wrapped_value
