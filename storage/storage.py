import time
from fastapi import FastAPI, Query
import elastic
from typing import List, Optional

# Approx. 2 minutes wait for startup in case elastic is starting in parallel
RETRY_CONNECTION = 120

app = FastAPI()

storage = elastic.StorageClient()
@app.on_event("startup")
async def startup_event():
    storage.wait_for_connection(RETRY_CONNECTION)
    storage.check_indices()

@app.post("/get_ds")
async def get_ds(ds_name: str, ev_name: str, ev_type: int, pv_list: List[str] = Query(...)):
    # Accept also a comma separated list
    if len(pv_list)==1 and pv_list[0].count(','):
        pv_list = pv_list[0].split(',')

    ds_id = await storage.get_dataset_id(ds_name, ev_name, ev_type, pv_list)
    
    return {'id': ds_id}

@app.post("/add_event")
async def add_event(ds_id: str, ev_timestamp: int, tg_pulse_id: int, path: str, expire_in: Optional[int] = 0):
    ev_id = storage.add_event(ds_id, ev_timestamp, tg_pulse_id, path)

    if expire_in > 0:
        expire_by = time.time_ns() + expire_in * int(1e9)
        storage.add_expire_by(ev_id, expire_by)
    
    return {'id': ev_id}