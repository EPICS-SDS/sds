#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from multiprocessing import cpu_count, get_context

import numpy as np
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV
from p4p.nt import NTScalar

from ntndarraywithevent import NTNDArrayWithEvent

class MyHandler(object):
    def __init__(self, event):
        self.event = event

    def put(self, pv, op):
        if op.value().raw.value is True:
            self.event.set()

        op.done()

class MyServer(object):
    def __init__(self, event):
        self.event = event
        
        self.stop_flag = False
        self.pvdb = dict()
        self.process = None
        self.mp_ctxt = get_context("fork")

        self.provider = StaticProvider('test')

    def add_pv(self, pv_name, n_elem, prefix):
        print(prefix+pv_name)
        pv = SharedPV(nt=NTNDArrayWithEvent(), initial=np.zeros(n_elem))
        self.provider.add(prefix+pv_name, pv)
        self.pvdb[pv_name] = pv

    def start_server(self):
        if self.pvdb != dict():
            self.process = self.mp_ctxt.Process(
                target=self._start_server)
            self.process.start()

    def _start_server(self):
        server = Server(providers=[self.provider])

        counter = 0
      
        with server:
            while not self.stop_flag:
                if self.stop_flag:
                    break
                counter += 1

                self.event.wait()
                self.event.clear()

                for pv in self.pvdb:
                    arr = np.random.random(self.pvdb[pv].current().shape[0])
                    arr[0] = counter

                    try:
                        self.pvdb[pv].post({'value': arr, 'pulse_id': counter, 'event_name': 'test', 'event_type':10})
                    except Exception:
                        pass

    def join(self):
        if self.process is not None:
            self.process.join()


def run_server(scenario, prefix):
    mp_ctxt = get_context("fork")

    mngr = mp_ctxt.Manager()
    event = mngr.Event()

    provider = StaticProvider('trigger')
    trigger_pv = SharedPV(handler=MyHandler(event), nt=NTScalar("?"), initial=False)
    provider.add(prefix+'TRIG', trigger_pv)

    servers = []
    for i in range(cpu_count()):
        servers.append(MyServer(event))

    next_server = 0
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

    for server in servers:
        server.start_server()

    Server.forever(providers=[provider])

    for server in servers:
        server.join()


if __name__ == "__main__":
    import sys

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
