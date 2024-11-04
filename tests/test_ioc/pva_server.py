#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
from multiprocessing import cpu_count, get_context, shared_memory
from threading import Thread

import numpy as np
from p4p.nt import NTScalar
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV

from test_ioc.ntscalararraysds import NTScalarArraySDS
from test_ioc.sds_pv import SdsPV


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


class MyServer(object):
    def __init__(self, event, n_cycles, freq):
        self.event = event
        self.n_cycles = n_cycles
        self.freq = freq

        self.pvdb = dict()
        self.process = None
        self.mp_ctxt = get_context("spawn")
        self.stop_flag = self.mp_ctxt.Event()
        self.queue = self.mp_ctxt.Queue()
        self.pvdb_lock = self.mp_ctxt.Lock()

    def add_pv(self, pv_name, n_elem, prefix):
        self.queue.put(("add", pv_name, n_elem, prefix))

    def _add_pv(self, pv_name, n_elem, prefix):
        with self.pvdb_lock:
            print(prefix + pv_name)
            pv = SharedPV(nt=NTScalarArraySDS("ad"), initial=np.zeros(n_elem))
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
        cycle_id = 0
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

                sds_event_cycle_id = cycle_id
                sds_event_timestamp = time.time_ns()
                for i in range(int(self.n_cycles[0])):
                    with self.pvdb_lock:
                        for pv in self.pvdb:
                            arr = np.random.random(self.pvdb[pv].current().shape[0])
                            arr[0] = cycle_id

                            try:
                                sds_pv = SdsPV(
                                    pv_ts=time.time_ns(),
                                    pv_value=arr,
                                    pv_type="ad",
                                    pv_name=pv,
                                    start_event_cycle_id=cycle_id,
                                    start_event_ts=time.time_ns(),
                                    main_event_cycle_id=cycle_id,
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
                                    sds_cycle_id=sds_event_cycle_id,
                                )
                                self.pvdb[pv].post(sds_pv)
                            except Exception as e:
                                print("error received", e)
                                pass
                        # Wait to process the next cycle at the right frequency
                        if i < int(self.n_cycles[0]) - 1:
                            time.sleep(1 / self.freq[0])
                        else:
                            time.sleep(0.001)
                        cycle_id += 1

        print("Server stopped")
        queue_thread.join()

    def join(self):
        if self.process is not None:
            self.process.join()


def run_server(n_pvs, n_elem, prefix):
    mp_ctxt = get_context("spawn")
    N_PROC = cpu_count()

    mngr = mp_ctxt.Manager()
    events = [mngr.Event() for i in range(N_PROC)]
    n_cycles = shared_memory.ShareableList([1])
    freq = shared_memory.ShareableList([14])

    servers = []
    for i in range(N_PROC):
        servers.append(MyServer(events[i], n_cycles, freq))

    provider = StaticProvider("trigger")
    trigger_pv = SharedPV(
        handler=TriggerHandler(events), nt=NTScalar("?"), initial=False
    )
    n_cycles_pv = SharedPV(handler=ScalarHandler(n_cycles), nt=NTScalar("i"), initial=1)
    freq_pv = SharedPV(handler=ScalarHandler(freq), nt=NTScalar("d"), initial=14.0)

    provider.add(prefix + "N_CYCLES", n_cycles_pv)
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
    for event in events:
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
        server.event.set()

    return pvs


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        n_pvs = int(sys.argv[1])
        n_elem = int(sys.argv[2])
    else:
        n_pvs = 10
        n_elem = 100

    prefix = "SDS:TEST:"

    servers = run_server(n_pvs, n_elem, prefix)
