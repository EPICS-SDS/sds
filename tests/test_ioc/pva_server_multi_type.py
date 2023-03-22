#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import string
import time
from multiprocessing import cpu_count, get_context, shared_memory
from threading import Thread

import numpy as np
from ntscalararraysds import NTScalarArraySDS
from numpy.random import rand, randint
from p4p.nt import NTScalar
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV
from sds_pv import SdsPV

letters = string.ascii_letters


class Choices:
    instances = dict()

    def __init__(self, length):
        n_letters = int(np.ceil(np.log(length) / np.log(len(letters))))
        if n_letters == 1:
            self.choices_list = [letters[randint(len(letters))] for i in range(length)]
        else:
            self.choices_list = [
                "".join(
                    [
                        letters[i]
                        for i in randint(
                            len(letters),
                            size=n_letters,
                        ).tolist()
                    ]
                )
                for i in range(length)
            ]

    @classmethod
    def get_choices(cls, len):
        if len not in cls.instances:
            cls.instances[len] = Choices(len)
        return cls.instances[len].choices_list


generators = {
    "b": lambda l: randint(low=-(2**7), high=2**7 - 1, size=l)[0],
    "h": lambda l: randint(low=-(2**15), high=2**15 - 1, size=l)[0],
    "i": lambda l: randint(low=-(2**31), high=2**31 - 1, size=l)[0],
    "l": lambda l: randint(low=-(2**63), high=2**63 - 1, size=l)[0],
    "B": lambda l: randint(low=0, high=2**7 - 1, size=l, dtype=np.uint8)[0],
    "H": lambda l: randint(low=0, high=2**15 - 1, size=l, dtype=np.uint16)[0],
    "I": lambda l: randint(low=0, high=2**31 - 1, size=l, dtype=np.uint32)[0],
    "L": lambda l: randint(low=0, high=2**63 - 1, size=l, dtype=np.uint64)[0],
    "ab": lambda l: randint(low=-(2**7), high=2**7 - 1, size=l, dtype=np.int8),
    "ah": lambda l: randint(low=-(2**15), high=2**15 - 1, size=l, dtype=np.int16),
    "ai": lambda l: randint(low=-(2**31), high=2**31 - 1, size=l, dtype=np.int32),
    "al": lambda l: randint(low=-(2**63), high=2**63 - 1, size=l),
    "aB": lambda l: randint(low=0, high=2**8 - 1, size=l, dtype=np.uint8),
    "aH": lambda l: randint(low=0, high=2**16 - 1, size=l, dtype=np.uint16),
    "aI": lambda l: randint(low=0, high=2**32 - 1, size=l, dtype=np.uint32),
    "aL": lambda l: randint(low=0, high=2**64 - 1, size=l, dtype=np.uint64),
    "f": lambda l: rand(),
    "d": lambda l: rand(),
    "af": lambda l: np.array(rand(l), dtype=np.float32),
    "ad": lambda l: rand(l),
    "s": lambda l: "".join(
        [
            letters[i]
            for i in randint(
                len(letters),
                size=l,
            ).tolist()
        ]
    ),
    "as": lambda l: [
        "".join(
            [
                letters[i]
                for i in randint(
                    len(letters),
                    size=l,
                ).tolist()
            ]
        )
        for i in range(l)
    ],
    "S": lambda l: {
        "value": {
            "index": int(randint(l)),
            "choices": Choices.get_choices(l),
        }
    },
}


class TriggerHandler(object):
    def __init__(self, events):
        self.events = events

    def put(self, pv, op):
        if op.value().raw.value is True:
            for event in self.events:
                event.set()

        op.done()


class ScalarHandler(object):
    def __init__(self, value):
        self.value = value

    def put(self, pv, op):
        if op.value().raw.value > 0:
            pv.post(op.value())
            self.value[0] = op.value().raw.value

        op.done()


class PutHandler(object):
    def put(self, pv, op):
        pulse_id = pv.current().pulseId.value + 1
        sds_event_timestamp = time.time_ns()
        sds_event_pulse_id = pulse_id
        sds_pv = SdsPV(
            pv_ts=time.time_ns(),
            pv_value=op.value(),
            pv_type="None",  # Not really needed here
            pv_name="name",
            start_event_pulse_id=pulse_id,
            start_event_ts=time.time_ns(),
            main_event_pulse_id=pulse_id,
            main_event_ts=time.time_ns(),
            acq_event_name="TestAcqEvent",
            acq_event_code=0,
            acq_event_delay=0,
            beam_mode="TestMode",
            beam_state="ON",
            beam_present="YES",
            beam_len=2.86e-3,
            beam_energy=2e9,
            beam_dest="Target",
            beam_curr=62.5e-3,
            sds_evt_code=1,
            sds_ts=sds_event_timestamp,
            sds_pulse_id=sds_event_pulse_id,
        )

        pv.post(sds_pv)
        op.done()


class MyServer(object):
    def __init__(self, event, n_pulses, freq):
        self.event = event
        self.n_pulses = n_pulses
        self.freq = freq

        self.pvdb = dict()
        self.process = None
        self.mp_ctxt = get_context("spawn")
        self.stop_flag = self.mp_ctxt.Event()
        self.queue = self.mp_ctxt.Queue()
        self.pvdb_lock = self.mp_ctxt.Lock()

    def add_pv(self, pv_name, pv_type, pv_len, prefix):
        self.queue.put(("add", pv_name, pv_type, pv_len, prefix))

    def _add_pv(self, pv_name, pv_type, pv_len, prefix):
        with self.pvdb_lock:
            print(prefix + pv_name)
            if isinstance(pv_type, tuple):
                initial_value = generators["S"](pv_len)
            else:
                initial_value = generators[pv_type](pv_len)
            pv = SharedPV(
                nt=NTScalarArraySDS(pv_type),
                unwrap=lambda x: x,
                handler=PutHandler(),
                initial=initial_value,
            )
            self.provider.add(prefix + pv_name, pv)
            self.pvdb[pv_name] = (pv, pv_type, pv_len)

    def stop(self):
        self.stop_flag.set()

    def start_server(self):
        self.process = self.mp_ctxt.Process(target=self._start_server)
        self.process.start()

    def _loop(self):
        while not self.stop_flag.is_set():
            command, *args = self.queue.get()
            if command == "add":
                self._add_pv(*args)

    def _start_server(self):
        pulse_id = 0
        self.provider = StaticProvider()

        server = Server(providers=[self.provider])

        queue_thread = Thread(target=self._loop)
        queue_thread.start()

        with server:
            while not self.stop_flag.is_set():
                self.event.wait()
                self.event.clear()
                if self.stop_flag.is_set():
                    break

                sds_event_pulse_id = pulse_id
                sds_event_timestamp = time.time_ns()
                for i in range(int(self.n_pulses[0])):
                    with self.pvdb_lock:
                        for pv in self.pvdb:
                            pv_type = self.pvdb[pv][1]
                            if isinstance(pv_type, tuple):
                                pv_type = "S"

                            pv_value = generators[pv_type](self.pvdb[pv][2])

                            try:
                                sds_pv = SdsPV(
                                    pv_ts=time.time_ns(),
                                    pv_value=pv_value,
                                    pv_type=pv_type,
                                    pv_name=pv,
                                    start_event_pulse_id=pulse_id,
                                    start_event_ts=time.time_ns(),
                                    main_event_pulse_id=pulse_id,
                                    main_event_ts=time.time_ns(),
                                    acq_event_name="TestAcqEvent",
                                    acq_event_code=0,
                                    acq_event_delay=0,
                                    beam_mode="TestMode",
                                    beam_state="ON",
                                    beam_present="YES",
                                    beam_len=2.86e-3,
                                    beam_energy=2e9,
                                    beam_dest="Target",
                                    beam_curr=62.5e-3,
                                    sds_evt_code=1,
                                    sds_ts=sds_event_timestamp,
                                    sds_pulse_id=sds_event_pulse_id,
                                )
                                self.pvdb[pv][0].post(sds_pv)
                            except Exception as e:
                                print("error received", e)
                                pass
                        # Wait to process the next pulse at the right frequency
                        if i < int(self.n_pulses[0]) - 1:
                            time.sleep(1 / self.freq[0])
                        else:
                            time.sleep(0.001)
                        pulse_id += 1

        print("Server stopped")
        queue_thread.join()

    def join(self):
        if self.process is not None:
            self.process.join()


def run_server(prefix, pvs_def):
    mp_ctxt = get_context("spawn")
    N_PROC = cpu_count()

    mngr = mp_ctxt.Manager()
    events = [mngr.Event() for i in range(N_PROC)]
    n_pulses = shared_memory.ShareableList([1])
    freq = shared_memory.ShareableList([14])

    servers = []
    for i in range(N_PROC):
        servers.append(MyServer(events[i], n_pulses, freq))

    provider = StaticProvider("trigger")
    trigger_pv = SharedPV(
        handler=TriggerHandler(events), nt=NTScalar("?"), initial=False
    )
    n_pulses_pv = SharedPV(handler=ScalarHandler(n_pulses), nt=NTScalar("i"), initial=1)
    freq_pv = SharedPV(handler=ScalarHandler(freq), nt=NTScalar("d"), initial=14.0)

    provider.add(prefix + "N_PULSES", n_pulses_pv)
    provider.add(prefix + "TRIG", trigger_pv)
    provider.add(prefix + "FREQ", freq_pv)

    pvs = create_pvs(servers, pvs_def, prefix)

    pvs_pv = SharedPV(nt=NTScalar("as"), initial=pvs)
    provider.add(prefix + "PVS", pvs_pv)

    for server in servers:
        server.start_server()

    # Set the event to load meaningful data from the start
    for event in events:
        event.set()

    with Server(providers=[provider]):
        for server in servers:
            server.join()


def create_pvs(servers, pvs_def, prefix):
    next_server = 0

    pv_names = []
    for pv in pvs_def:
        pv_name = pv
        pv_type = pvs_def[pv]["type"]
        pv_len = pvs_def[pv]["len"]

        servers[next_server].add_pv(pv_name, pv_type, pv_len, prefix)
        next_server += 1
        next_server %= len(servers)
        pv_names.append(prefix + pv_name)

    for server in servers:
        server.event.set()

    return pv_names


if __name__ == "__main__":

    prefix = "SDS:TYPES_TEST:"

    pvs_def = {
        "PV:STRING": {"type": "s", "len": 10},
        "PV:ASTRING": {"type": "as", "len": 10},
        "PV:BYTE": {"type": "b", "len": 1},
        "PV:SHORT": {"type": "h", "len": 1},
        "PV:INT": {"type": "i", "len": 1},
        "PV:LONG": {"type": "l", "len": 1},
        "PV:ABYTE": {"type": "ab", "len": 10},
        "PV:ASHORT": {"type": "ah", "len": 10},
        "PV:AINT": {"type": "ai", "len": 10},
        "PV:ALONG": {"type": "al", "len": 10},
        "PV:UBYTE": {"type": "B", "len": 1},
        "PV:USHORT": {"type": "H", "len": 1},
        "PV:UINT": {"type": "I", "len": 1},
        "PV:ULONG": {"type": "L", "len": 1},
        "PV:AUBYTE": {"type": "aB", "len": 10},
        "PV:AUSHORT": {"type": "aH", "len": 10},
        "PV:AUINT": {"type": "aI", "len": 10},
        "PV:AULONG": {"type": "aL", "len": 10},
        "PV:FLOAT": {"type": "f", "len": 1},
        "PV:DOUBLE": {"type": "d", "len": 1},
        "PV:AFLOAT": {"type": "af", "len": 10},
        "PV:ADOUBLE": {"type": "ad", "len": 10},
        "PV:ENUM": {
            "type": ("S", "enum_t", [("index", "i"), ("choices", "as")]),
            "len": 5,
        },
    }

    servers = run_server(prefix, pvs_def)
