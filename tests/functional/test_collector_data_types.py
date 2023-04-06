import asyncio
import subprocess
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
from h5py import File
from p4p.client.asyncio import Context
from p4p.client.thread import Context as ThContext
from tests.functional.service_loader import collector_service, indexer_service

from esds.collector.config import settings
from esds.common.files.config import settings as file_settings

# PV names
FLOAT_PV = "SDS:TYPES_TEST:PV:FLOAT"
DOUBLE_PV = "SDS:TYPES_TEST:PV:DOUBLE"
BYTE_PV = "SDS:TYPES_TEST:PV:BYTE"
SHORT_PV = "SDS:TYPES_TEST:PV:SHORT"
INT_PV = "SDS:TYPES_TEST:PV:INT"
LONG_PV = "SDS:TYPES_TEST:PV:LONG"
UBYTE_PV = "SDS:TYPES_TEST:PV:UBYTE"
USHORT_PV = "SDS:TYPES_TEST:PV:USHORT"
UINT_PV = "SDS:TYPES_TEST:PV:UINT"
ULONG_PV = "SDS:TYPES_TEST:PV:ULONG"
STRING_PV = "SDS:TYPES_TEST:PV:STRING"
ENUM_PV = "SDS:TYPES_TEST:PV:ENUM"


AFLOAT_PV = "SDS:TYPES_TEST:PV:AFLOAT"
ADOUBLE_PV = "SDS:TYPES_TEST:PV:ADOUBLE"
ABYTE_PV = "SDS:TYPES_TEST:PV:ABYTE"
ASHORT_PV = "SDS:TYPES_TEST:PV:ASHORT"
AINT_PV = "SDS:TYPES_TEST:PV:AINT"
ALONG_PV = "SDS:TYPES_TEST:PV:ALONG"
AUBYTE_PV = "SDS:TYPES_TEST:PV:AUBYTE"
AUSHORT_PV = "SDS:TYPES_TEST:PV:AUSHORT"
AUINT_PV = "SDS:TYPES_TEST:PV:AUINT"
AULONG_PV = "SDS:TYPES_TEST:PV:AULONG"
ASTRING_PV = "SDS:TYPES_TEST:PV:ASTRING"

# Test values
FLOAT_1 = 2 ** (2**8 - 1 - 2**7 - 1) * (
    1 + np.sum([2 ** (-i - 1) for i in range(23)])
)
FLOAT_2 = -(2 ** (-(2**7) - 1)) * (1 + 2 ** (-23))
DOUBLE_1 = 2 ** (2**11 - 1 - 2**10 - 1) * (
    1 + np.sum([2 ** (-i - 1) for i in range(53)])
)
DOUBLE_2 = -(2 ** (-(2**10) - 1)) * (1 + 2 ** (-53))

BYTE_1 = 2**7 - 1
SHORT_1 = 2**15 - 1
INT_1 = 2**31 - 1
LONG_1 = 2**63 - 1

BYTE_2 = -(2**7)
SHORT_2 = -(2**15)
INT_2 = -(2**31)
LONG_2 = -(2**63)

UBYTE_1 = 2**8 - 1
USHORT_1 = 2**16 - 1
UINT_1 = 2**32 - 1
ULONG_1 = 2**64 - 1

UBYTE_2 = 2**7 - 1
USHORT_2 = 2**15 - 1
UINT_2 = 2**31 - 1
ULONG_2 = 2**63 - 1


STRING_1 = b"test_1"
STRING_2 = b"test_2"

ENUM_1 = 1
ENUM_2 = 2


class TestCollector:
    @classmethod
    def setup_class(cls):
        cls.p = subprocess.Popen(
            ["python", "test_ioc/pva_server_multi_type.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Waiting to connect to the SDS:TEST:TRIG, which is the last one to be created
        ctxt = ThContext()
        ctxt.get("SDS:TYPES_TEST:TRIG", timeout=15)

        ctxt.close()

    @classmethod
    def teardown_class(cls):
        cls.p.kill()

    async def wait_for_pv_value(self, new_value):
        queue = asyncio.Queue()
        mon = None

        async def cb(value):
            if value[0] == new_value:
                await queue.put(value)
                if mon is not None:
                    mon.close()

        ctxt = Context()
        mon = ctxt.monitor(self.test_pv, cb=cb)
        return mon, queue

    @pytest.mark.asyncio
    async def test_acquisition(self, indexer_service, collector_service):
        with Context() as ctxt:
            # Floating point (max value)
            await ctxt.put(FLOAT_PV, FLOAT_1)
            await ctxt.put(DOUBLE_PV, DOUBLE_1)
            # Testing positive numbers of signed integers
            await ctxt.put(BYTE_PV, BYTE_1)
            await ctxt.put(SHORT_PV, SHORT_1)
            await ctxt.put(INT_PV, INT_1)
            await ctxt.put(LONG_PV, LONG_1)
            # Testing higher half range of unsigned integers
            await ctxt.put(UBYTE_PV, UBYTE_1)
            await ctxt.put(USHORT_PV, USHORT_1)
            await ctxt.put(UINT_PV, UINT_1)
            await ctxt.put(ULONG_PV, ULONG_1)
            # String
            await ctxt.put(STRING_PV, STRING_1)
            # Enum 1
            await ctxt.put(ENUM_PV, ENUM_1)
            # Arrays
            await ctxt.put(AFLOAT_PV, [FLOAT_1, FLOAT_2])
            await ctxt.put(ADOUBLE_PV, [DOUBLE_1, DOUBLE_2])
            await ctxt.put(ABYTE_PV, [BYTE_1, BYTE_2])
            await ctxt.put(ASHORT_PV, [SHORT_1, SHORT_2])
            await ctxt.put(AINT_PV, [INT_1, INT_2])
            await ctxt.put(ALONG_PV, [LONG_1, LONG_2])
            await ctxt.put(AUBYTE_PV, [UBYTE_1, UBYTE_2])
            await ctxt.put(AUSHORT_PV, [USHORT_1, USHORT_2])
            await ctxt.put(AUINT_PV, [UINT_1, UINT_2])
            await ctxt.put(AULONG_PV, [ULONG_1, ULONG_2])
            await ctxt.put(ASTRING_PV, [STRING_1, STRING_2])

            await asyncio.sleep(1)
            # Second "pulse"
            # Floating point (max negative value)
            await ctxt.put(FLOAT_PV, FLOAT_2)
            await ctxt.put(DOUBLE_PV, DOUBLE_2)
            # Testing negative numbers of signed integers
            await ctxt.put(BYTE_PV, BYTE_2)
            await ctxt.put(SHORT_PV, SHORT_2)
            await ctxt.put(INT_PV, INT_2)
            await ctxt.put(LONG_PV, LONG_2)
            # Testing lower half range of unsigned integers
            await ctxt.put(UBYTE_PV, UBYTE_2)
            await ctxt.put(USHORT_PV, USHORT_2)
            await ctxt.put(UINT_PV, UINT_2)
            await ctxt.put(ULONG_PV, ULONG_2)
            # String
            await ctxt.put(STRING_PV, STRING_2)
            # Enum 2
            await ctxt.put(ENUM_PV, ENUM_2)
            # Arrays
            await ctxt.put(AFLOAT_PV, [FLOAT_2, FLOAT_1])
            await ctxt.put(ADOUBLE_PV, [DOUBLE_2, DOUBLE_1])
            await ctxt.put(ABYTE_PV, [BYTE_2, BYTE_1])
            await ctxt.put(ASHORT_PV, [SHORT_2, SHORT_1])
            await ctxt.put(AINT_PV, [INT_2, INT_1])
            await ctxt.put(ALONG_PV, [LONG_2, LONG_1])
            await ctxt.put(AUBYTE_PV, [UBYTE_2, UBYTE_1])
            await ctxt.put(AUSHORT_PV, [USHORT_2, USHORT_1])
            await ctxt.put(AUINT_PV, [UINT_2, UINT_1])
            await ctxt.put(AULONG_PV, [ULONG_2, ULONG_1])
            await ctxt.put(ASTRING_PV, [STRING_2, STRING_1])

            await asyncio.sleep(settings.collector_timeout + 5)

    # Tests for floats
    def test_float(self):
        self.check_value(pulse=1, pv=FLOAT_PV, dtype=np.float32, value=FLOAT_1)
        self.check_value(pulse=2, pv=FLOAT_PV, dtype=np.float32, value=FLOAT_2)

    def test_double(self):
        self.check_value(pulse=1, pv=DOUBLE_PV, dtype=np.float64, value=DOUBLE_1)
        self.check_value(pulse=2, pv=DOUBLE_PV, dtype=np.float64, value=DOUBLE_2)

    # Tests for signed integers
    def test_byte(self):
        self.check_value(pulse=1, pv=BYTE_PV, dtype=np.int8, value=BYTE_1)
        self.check_value(pulse=2, pv=BYTE_PV, dtype=np.int8, value=BYTE_2)

    def test_short(self):
        self.check_value(pulse=1, pv=SHORT_PV, dtype=np.int16, value=SHORT_1)
        self.check_value(pulse=2, pv=SHORT_PV, dtype=np.int16, value=SHORT_2)

    def test_int(self):
        self.check_value(pulse=1, pv=INT_PV, dtype=np.int32, value=INT_1)
        self.check_value(pulse=2, pv=INT_PV, dtype=np.int32, value=INT_2)

    def test_long(self):
        self.check_value(pulse=1, pv=LONG_PV, dtype=np.int64, value=LONG_1)
        self.check_value(pulse=2, pv=LONG_PV, dtype=np.int64, value=LONG_2)

    # Tests for unsigned integers
    def test_ubyte(self):
        self.check_value(pulse=1, pv=UBYTE_PV, dtype=np.uint8, value=UBYTE_1)
        self.check_value(pulse=2, pv=UBYTE_PV, dtype=np.uint8, value=UBYTE_2)

    def test_ushort(self):
        self.check_value(pulse=1, pv=USHORT_PV, dtype=np.uint16, value=USHORT_1)
        self.check_value(pulse=2, pv=USHORT_PV, dtype=np.uint16, value=USHORT_2)

    def test_uint(self):
        self.check_value(pulse=1, pv=UINT_PV, dtype=np.uint32, value=UINT_1)
        self.check_value(pulse=2, pv=UINT_PV, dtype=np.uint32, value=UINT_2)

    def test_ulong(self):
        self.check_value(pulse=1, pv=ULONG_PV, dtype=np.uint64, value=ULONG_1)
        self.check_value(pulse=2, pv=ULONG_PV, dtype=np.uint64, value=ULONG_2)

    # Tests for string
    def test_string(self):
        self.check_value(pulse=1, pv=STRING_PV, dtype=np.object_, value=STRING_1)
        self.check_value(pulse=2, pv=STRING_PV, dtype=np.object_, value=STRING_2)

    # Tests for enum
    def test_enum(self):
        self.check_value(pulse=1, pv=ENUM_PV, dtype=np.int32, value=ENUM_1)
        self.check_value(pulse=2, pv=ENUM_PV, dtype=np.int32, value=ENUM_2)

    # Arrays

    def test_afloat(self):
        self.check_value(
            pulse=1, pv=AFLOAT_PV, dtype=np.float32, value=[FLOAT_1, FLOAT_2]
        )
        self.check_value(
            pulse=2, pv=AFLOAT_PV, dtype=np.float32, value=[FLOAT_2, FLOAT_1]
        )

    def test_adouble(self):
        self.check_value(
            pulse=1, pv=ADOUBLE_PV, dtype=np.float64, value=[DOUBLE_1, DOUBLE_2]
        )
        self.check_value(
            pulse=2, pv=ADOUBLE_PV, dtype=np.float64, value=[DOUBLE_2, DOUBLE_1]
        )

    def test_abyte(self):
        self.check_value(pulse=1, pv=ABYTE_PV, dtype=np.int8, value=[BYTE_1, BYTE_2])
        self.check_value(pulse=2, pv=ABYTE_PV, dtype=np.int8, value=[BYTE_2, BYTE_1])

    def test_ashort(self):
        self.check_value(
            pulse=1, pv=ASHORT_PV, dtype=np.int16, value=[SHORT_1, SHORT_2]
        )
        self.check_value(
            pulse=2, pv=ASHORT_PV, dtype=np.int16, value=[SHORT_2, SHORT_1]
        )

    def test_aint(self):
        self.check_value(pulse=1, pv=AINT_PV, dtype=np.int32, value=[INT_1, INT_2])
        self.check_value(pulse=2, pv=AINT_PV, dtype=np.int32, value=[INT_2, INT_1])

    def test_along(self):
        self.check_value(pulse=1, pv=ALONG_PV, dtype=np.int64, value=[LONG_1, LONG_2])
        self.check_value(pulse=2, pv=ALONG_PV, dtype=np.int64, value=[LONG_2, LONG_1])

    def test_aubyte(self):
        self.check_value(
            pulse=1, pv=AUBYTE_PV, dtype=np.uint8, value=[UBYTE_1, UBYTE_2]
        )
        self.check_value(
            pulse=2, pv=AUBYTE_PV, dtype=np.uint8, value=[UBYTE_2, UBYTE_1]
        )

    def test_aushort(self):
        self.check_value(
            pulse=1, pv=AUSHORT_PV, dtype=np.uint16, value=[USHORT_1, USHORT_2]
        )
        self.check_value(
            pulse=2, pv=AUSHORT_PV, dtype=np.uint16, value=[USHORT_2, USHORT_1]
        )

    def test_auint(self):
        self.check_value(pulse=1, pv=AUINT_PV, dtype=np.uint32, value=[UINT_1, UINT_2])
        self.check_value(pulse=2, pv=AUINT_PV, dtype=np.uint32, value=[UINT_2, UINT_1])

    def test_aulong(self):
        self.check_value(
            pulse=1, pv=AULONG_PV, dtype=np.uint64, value=[ULONG_1, ULONG_2]
        )
        self.check_value(
            pulse=2, pv=AULONG_PV, dtype=np.uint64, value=[ULONG_2, ULONG_1]
        )

    def test_astring(self):
        self.check_value(
            pulse=1, pv=ASTRING_PV, dtype=np.object_, value=[STRING_1, STRING_2]
        )
        self.check_value(
            pulse=2, pv=ASTRING_PV, dtype=np.object_, value=[STRING_2, STRING_1]
        )

    def check_value(self, pulse, pv, dtype, value):
        directory = Path(
            datetime.utcnow().strftime("%Y"),
            datetime.utcnow().strftime("%Y-%m-%d"),
        )
        file_path = file_settings.storage_path / directory / f"types-test_3_{pulse}.h5"

        assert file_path.exists(), f"File {file_path} not found."

        h5file = File(file_path, "r")
        entry = h5file.get("entry", None)
        assert entry is not None, f"File {file_path} does not contain an entry group."

        sds_event = entry.get(f"sds_event_{pulse}", None)
        assert (
            sds_event is not None
        ), f"File {file_path} does not contain the sds event for pulse {pulse}."

        pulse = sds_event.get(f"pulse_{pulse}", None)
        assert (
            pulse is not None
        ), f"File {file_path} does not contain the pulse {pulse}."

        pv_field = pulse.get(pv, None)
        assert (
            pv_field is not None
        ), f"PV {pv} not found in file {file_path} for pulse {pulse}"

        assert pv_field.dtype == dtype

        if isinstance(value, list):
            assert (pv_field[()] == np.array(value, dtype=dtype)).all()
        else:
            assert pv_field[()] == (value if dtype is np.object_ else dtype(value))

        h5file.close()
