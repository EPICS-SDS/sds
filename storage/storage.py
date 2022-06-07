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


@app.post("/get_collector")
async def get_collector(name: str, method_name: str, method_type: int,
                        pv_list: List[str] = Query(...)):
    # Accept also a comma separated list
    if len(pv_list) == 1 and pv_list[0].count(","):
        pv_list = pv_list[0].split(",")

    id = await storage.get_collector_id(name, method_name, method_type,
                                        pv_list)

    return {"id": id}


@app.post("/add_dataset")
async def add_dataset(collector_id: str, dataset_timestamp: int,
                      tg_pulse_id: int, path: str,
                      expire_in: Optional[int] = 0):
    dataset_id = storage.add_dataset(
        collector_id, dataset_timestamp, tg_pulse_id, path)

    if expire_in > 0:
        expire_by = time.time_ns() + expire_in * int(1e9)
        storage.add_expire_by(dataset_id, expire_by)

    return {"id": dataset_id}
