#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
from multiprocessing import cpu_count, get_context, shared_memory

import numpy as np
from p4p.nt import NTScalar
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV

from ntndarraywithevent import NTNDArrayWithEvent


class TriggerHandler(object):
    def __init__(self, events):
        self.events = events

    def put(self, pv, op):
        if op.value().raw.value is True:
            for (lock, event) in self.events:
                with lock:
                    event.set()

        op.done()


class NPulsesHandler(object):
    def __init__(self, n_pulses):
        self.n_pulses = n_pulses

    def put(self, pv, op):
        if op.value().raw.value > 0:
            pv.post(op.value())
            self.n_pulses.buf[0] = op.value().raw.value

        op.done()


class MyServer(object):
    def __init__(self, lock, event, n_pulses):
        self.lock = lock
        self.event = event
        self.n_pulses = n_pulses

        self.stop_flag = False
        self.pvdb = dict()
        self.process = None
        self.mp_ctxt = get_context("fork")

        self.provider = StaticProvider("test")

    def add_pv(self, pv_name, n_elem, prefix):
        print(prefix + pv_name)
        pv = SharedPV(nt=NTNDArrayWithEvent(), initial=np.zeros(n_elem))
        self.provider.add(prefix + pv_name, pv)
        self.pvdb[pv_name] = pv

    def start_server(self):
        if self.pvdb != dict():
            self.process = self.mp_ctxt.Process(target=self._start_server)
            self.process.start()

    def _start_server(self):
        server = Server(providers=[self.provider])

        pulse_id = 0

        with server:
            while not self.stop_flag:
                if self.stop_flag:
                    break

                self.event.wait()
                with self.lock:
                    self.event.clear()

                trigger_pulse_id = pulse_id
                trigger_timestamp = time.time_ns()
                for i in range(int(self.n_pulses.buf[0])):
                    for pv in self.pvdb:
                        arr = np.random.random(self.pvdb[pv].current().shape[0])
                        arr[0] = pulse_id

                        try:
                            self.pvdb[pv].post(
                                {
                                    "value": arr,
                                    "trigger_pulse_id": trigger_pulse_id,
                                    "trigger_timestamp": trigger_timestamp,
                                    "timestamp": time.time_ns(),
                                    "pulse_id": pulse_id,
                                    "event_name": "data-on-demand",
                                    "event_code": 1,
                                }
                            )
                        except Exception:
                            pass
                    pulse_id += 1

    def join(self):
        if self.process is not None:
            self.process.join()


def run_server(scenario, prefix):
    mp_ctxt = get_context("fork")

    mngr = mp_ctxt.Manager()
    events = [(mngr.Lock(), mngr.Event()) for i in range(cpu_count())]
    n_pulses = shared_memory.SharedMemory(create=True, size=sys.getsizeof(1))
    n_pulses.buf[0] = 1

    servers = []
    for i in range(cpu_count()):
        servers.append(MyServer(*events[i], n_pulses))

    provider = StaticProvider("trigger")
    trigger_pv = SharedPV(
        handler=TriggerHandler(events), nt=NTScalar("?"), initial=False
    )
    n_pulses_pv = SharedPV(
        handler=NPulsesHandler(n_pulses), nt=NTScalar("i"), initial=1
    )

    next_server = 0
    pvs = []
    for filename in scenario.keys():
        n_pvs = scenario[filename]["n_pvs"]
        n_elem = scenario[filename]["n_elem"]
        for i in range(len(n_pvs)):
            for j in range(n_pvs[i]):
                pv_name = filename
                if len(n_elem) > 1:
                    pv_name += "_" + str(n_elem[i])
                if n_pvs[i] > 1:
                    pv_name += "_" + str(j)

                servers[next_server].add_pv(pv_name, n_elem[i], prefix)
                next_server += 1
                next_server %= len(servers)
                pvs.append(pv_name)

    provider.add(prefix + "N_PULSES", n_pulses_pv)
    provider.add(prefix + "TRIG", trigger_pv)

    pvs_pv = SharedPV(nt=NTScalar("as"), initial=pvs)
    provider.add(prefix + "PVS", pvs_pv)

    for server in servers:
        server.start_server()

    # Set the event to load meaninful data from the start
    for (lock, event) in events:
        with lock:
            event.set()

    Server.forever(providers=[provider])

    for server in servers:
        server.join()


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        n_elem = [
            int(float(n))
            for n in (sys.argv[1].replace("[", "").replace("]", "").split(","))
        ]
        if sys.argv[2].count("[") == 0:
            n_pvs = [int(sys.argv[2])] * len(n_elem)
        else:
            n_pvs = [
                int(n)
                for n in (sys.argv[2].replace("[", "").replace("]", "").split(","))
            ]
    else:
        n_elem = [1, 10, 100, 1000, 10000, 100000, int(1e6)]
        n_pvs = [5] * len(n_elem)

    prefix = "SDS:TEST:"
    scenario = dict()
    for i in range(len(n_pvs)):
        for j in range(n_pvs[i]):
            filename = "PV_" + str(n_elem[i]) + "_" + str(j)
            scenario[filename] = dict()
            scenario[filename]["n_elem"] = [n_elem[i]]
            scenario[filename]["n_pvs"] = [1]

    event_size = 0
    for filename in scenario.keys():
        n_pvs = scenario[filename]["n_pvs"]
        n_elem = scenario[filename]["n_elem"]
        for i in range(len(n_pvs)):
            event_size += n_pvs[i] * n_elem[i] * 8

    print("The event size is {0:1.2f} MB".format(event_size * 1e-6))

    servers = run_server(scenario, prefix)
