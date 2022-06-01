import time
from fastapi import FastAPI, Query
from typing import List, Optional

## To be removed when proper package is published to artifactory
import sys
sys.path.append('../')

from common.elastic import ElasticClient

# Approx. 2 minutes wait for startup in case elastic is starting in parallel
RETRY_CONNECTION = 120

app = FastAPI()

es = ElasticClient()
@app.on_event("startup")
async def startup_event():
    es.wait_for_connection(RETRY_CONNECTION)
    es.create_mising_indices()

@app.post("/get_ds")
async def get_ds(ds_name: str, ev_name: str, ev_type: int, pv_list: List[str] = Query(...)):
    '''
    Get the ID for a dataset that matches all the parameters.
    If no dataset is found, a new one is created.

    Arguments:
    - **ds_name** (str, required): name of the dataset
    - **ev_name** (str, required): name of the event
    - **ev_type** (int, required): event type
    - **pv_list** (List[str], required): list of PVs in the dataset. It accept comma-separated list.
    
    Returns the dataset ID.
    '''
    # Accept also a comma separated list
    if len(pv_list)==1 and pv_list[0].count(','):
        pv_list = pv_list[0].split(',')

    ds_id = await es.get_dataset_id(ds_name, ev_name, ev_type, pv_list)
    
    return {'id': ds_id}

@app.post("/add_event")
async def add_event(ds_id: str, ev_timestamp: int, tg_pulse_id: int, path: str, expire_in: Optional[int] = 0):
    '''
    Add an event to the elasticsearch index.

    Arguments:
    - **ds_id** (str, required): dataset ID obtained using the /get_id POST operation
    - **ev_timestamp** (int, required): event timestamp in UTC
    - **tg_pulse_id** (int, required): pulse ID at the time of the trigger
    - **path** (str, required): path of the file containing the data
    - **expire_in** (str, optional): a time in seconds after which the data will be deleted 
    '''
    ev_id = es.add_event(ds_id, ev_timestamp, tg_pulse_id, path)

    if expire_in > 0:
        expire_by = time.time_ns() + expire_in * int(1e9)
        es.add_expire_by(ev_id, expire_by)
    
    return {'id': ev_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,
        host="0.0.0.0",
    )