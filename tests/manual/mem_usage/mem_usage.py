import json
import os
import subprocess
import time

from p4p.client.thread import Context

# Duration of the test
duration = 300  # seconds

# Simulating 1 second of RF data: 120 waveforms with 70k points @14 Hz

# Number of PVs to generate
n_pvs = 10
# Length of each array
pv_len = int(10e3)
# cycles per trigger
n_cycles = 30

events_per_file = 10

# triggering interval [s]
trig_interval = 5

os.environ["N_PVS"] = str(n_pvs)
os.environ["N_ELEM"] = str(pv_len)
os.environ["STORAGE_PATH"] = os.path.curdir + "/data"
os.environ["MEAS_TIME"] = str(duration)
os.environ["EVENTS_PER_FILE"] = str(events_per_file)

os.makedirs(os.environ["STORAGE_PATH"], exist_ok=True)

n_collectors = 1
collectors = [
    {
        "name": "collector_" + str(i),
        "event_name": "data-on-demand",
        "event_code": 1,
        "pvs": [
            f"SDS:TEST:PV_{pv_len}_{j}" if n_pvs > 1 else f"SDS:TEST:PV_{pv_len}"
            for j in range(n_pvs)
        ],
    }
    for i in range(n_collectors)
]
with open("collector.json", "w") as config:
    json.dump(collectors, config, indent=4)

ctxt = Context()

p = subprocess.Popen("docker compose up", shell=True)

# Wait for elastic to start
time.sleep(10)

# Wait for IOC to start
t0 = time.time()
while time.time() - t0 < 60:
    try:
        ctxt.get("SDS:TEST:TRIG")
    except TimeoutError:
        continue
    break

ctxt.put("SDS:TEST:N_CYCLES", n_cycles)

t0 = time.time()

while time.time() - t0 < duration:
    ctxt.put("SDS:TEST:TRIG", True)
    time.sleep(trig_interval)

time.sleep(1)

p2 = subprocess.Popen("docker compose down", shell=True)

p.wait()
p2.wait()
