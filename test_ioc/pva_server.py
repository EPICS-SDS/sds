#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from threading import Thread
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


class ScalarHandler(object):
    def __init__(self, value):
        self.value = value

    def put(self, pv, op):
        if op.value().raw.value > 0:
            pv.post(op.value())
            self.value[0] = op.value().raw.value

        op.done()


class MyServer(object):
    def __init__(self, lock, event, n_pulses, freq):
        self.lock = lock
        self.event = event
        self.n_pulses = n_pulses
        self.freq = freq

        self.pvdb = dict()
        self.process = None
        self.mp_ctxt = get_context("fork")
        self.stop_flag = self.mp_ctxt.Event()
        self.queue = self.mp_ctxt.Queue()
        self.pvdb_lock = self.mp_ctxt.Lock()

        self.provider = StaticProvider()

    def add_pv(self, pv_name, n_elem, prefix):
        self.queue.put(("add", pv_name, n_elem, prefix))

    def _add_pv(self, pv_name, n_elem, prefix):
        with self.pvdb_lock:
            print(prefix + pv_name)
            pv = SharedPV(nt=NTNDArrayWithEvent(), initial=np.zeros(n_elem))
            self.provider.add(prefix + pv_name, pv)
            self.pvdb[pv_name] = pv

    def clear_pvs(self):
        self.queue.put(("clear",))

    def _clear_pvs(self):
        with self.pvdb_lock:
            # Clear the PVs
            self.pvdb = dict()
            for pv in self.provider.keys():
                self.provider.remove(pv)

    def stop(self):
        self.stop_flag.set()

    def start_server(self):
        self.process = self.mp_ctxt.Process(target=self._start_server)
        self.process.start()

    def _loop(self):
        while not self.stop_flag.is_set():
            command, *args = self.queue.get()
            if command == "clear":
                self._clear_pvs()
            elif command == "add":
                self._add_pv(*args)

    def _start_server(self):
        pulse_id = 0

        server = Server(providers=[self.provider])

        queue_thread = Thread(target=self._loop)
        queue_thread.start()

        with server:
            while not self.stop_flag.is_set():
                self.event.wait()
                with self.lock:
                    self.event.clear()
                if self.stop_flag.is_set():
                    break

                trigger_pulse_id = pulse_id
                trigger_timestamp = time.time_ns()
                for i in range(int(self.n_pulses[0])):
                    with self.pvdb_lock:
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
                            if i < int(self.n_pulses[0]) - 1:
                                time.sleep(1 / self.freq[0])
                        pulse_id += 1

        print("Server stopped")
        queue_thread.join()

    def join(self):
        if self.process is not None:
            self.process.join()


def run_server(n_pvs, n_elem, prefix):
    mp_ctxt = get_context("fork")
    N_PROC = cpu_count()

    mngr = mp_ctxt.Manager()
    events = [(mngr.Lock(), mngr.Event()) for i in range(N_PROC)]
    n_pulses = shared_memory.ShareableList([1])
    freq = shared_memory.ShareableList([14])

    servers = []
    for i in range(N_PROC):
        servers.append(MyServer(*events[i], n_pulses, freq))

    provider = StaticProvider("trigger")
    trigger_pv = SharedPV(
        handler=TriggerHandler(events), nt=NTScalar("?"), initial=False
    )
    n_pulses_pv = SharedPV(handler=ScalarHandler(n_pulses), nt=NTScalar("i"), initial=1)
    freq_pv = SharedPV(handler=ScalarHandler(freq), nt=NTScalar("d"), initial=14.0)

    provider.add(prefix + "N_PULSES", n_pulses_pv)
    provider.add(prefix + "TRIG", trigger_pv)
    provider.add(prefix + "FREQ", freq_pv)

    pvs = update_pvs(servers, n_pvs, n_elem, prefix)

    pvs_pv = SharedPV(nt=NTScalar("as"), initial=pvs)
    provider.add(prefix + "PVS", pvs_pv)

    n_elem_pv = SharedPV(nt=NTScalar("i"), initial=1)
    n_pvs_pv = SharedPV(nt=NTScalar("i"), initial=1)

    @n_elem_pv.put
    @n_pvs_pv.put
    def onNPvsPut(pv, op):
        pv.post(op.value())
        op.done()
        n_pvs = n_pvs_pv.current().raw.value
        n_elem = n_elem_pv.current().raw.value

        pvs = update_pvs(servers, n_pvs, n_elem, prefix)
        pvs_pv.post(pvs)

    provider.add(prefix + "N_ELEM", n_elem_pv)
    provider.add(prefix + "N_PVS", n_pvs_pv)

    for server in servers:
        server.start_server()

    # Set the event to load meaningful data from the start
    for (lock, event) in events:
        with lock:
            event.set()

    with Server(providers=[provider]):
        for server in servers:
            server.join()


def update_pvs(servers, n_pvs, n_elem, prefix):
    for server in servers:
        server.clear_pvs()

    next_server = 0

    pvs = []
    for i in range(n_pvs):
        pv_name = "PV"
        pv_name += "_" + str(n_elem)
        if n_pvs > 1:
            pv_name += "_" + str(i)

        servers[next_server].add_pv(pv_name, n_elem, prefix)
        next_server += 1
        next_server %= len(servers)
        pvs.append(prefix + pv_name)

    for server in servers:
        with server.lock:
            server.event.set()

    return pvs


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        n_pvs = int(sys.argv[1])
        n_elem = int(sys.argv[2])
    else:
        n_pvs = 1
        n_elem = 1

    prefix = "SDS:TEST:"

    servers = run_server(n_pvs, n_elem, prefix)
