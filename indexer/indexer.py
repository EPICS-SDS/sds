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
    es.create_missing_indices()


@app.post("/get_collector")
async def get_collector(collector_name: str, event_name: str, event_code: int,
                        pv_list: List[str] = Query(...)):
    '''
    Get the ID for a collector that matches all the parameters.
    If no collector is found, a new one is created.

    Arguments:
    - **collector_name** (str, required): name of the collector
    - **event_name** (str, required): name of the event
    - **event_code** (int, required): event type
    - **pv_list** (List[str], required): list of PVs in the dataset. It accept comma-separated list.

    Returns the dataset ID.
    '''
    # Accept also a comma separated list
    if len(pv_list) == 1 and pv_list[0].count(','):
        pv_list = pv_list[0].split(',')

    collector_id = await es.get_collector_id(collector_name, event_name,
                                             event_code, pv_list)

    return {'id': collector_id}


@app.post("/add_dataset")
async def add_dataset(collector_id: str, event_timestamp: int,
                      trigger_pulse_id: int, path: str,
                      expire_in: Optional[int] = 0):
    '''
    Add an dataset to the elasticsearch index.

    Arguments:
    - **collector_id** (str, required): collector ID obtained using the /get_id POST operation
    - **event_timestamp** (int, required): event timestamp in UTC
    - **tg_pulse_id** (int, required): pulse ID at the time of the trigger
    - **path** (str, required): path of the file containing the data
    - **expire_in** (str, optional): a time in seconds after which the data will be deleted
    '''
    dataset_id = es.add_dataset(collector_id, event_timestamp,
                                trigger_pulse_id, path)

    if expire_in > 0:
        expire_by = time.time_ns() + expire_in * int(1e9)
        es.add_expire_by(dataset_id, expire_by)

    return {'id': dataset_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0")
