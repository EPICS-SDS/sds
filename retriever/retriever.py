## To be removed when proper package is published to artifactory
import os
import sys
import zipfile
from io import BytesIO
from typing import List, Optional
from memory_nexus import MemoryNexus

import h5py as hp
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse

sys.path.append("../")

from common.elastic import ElasticClient

# Approx. 2 minutes wait for startup in case elastic is starting in parallel
RETRY_CONNECTION = 0

DATA_STORAGE = os.environ.get("DATA_STORAGE", ".")

app = FastAPI()

es = ElasticClient()


@app.on_event("startup")
async def startup_event():
    es.wait_for_connection(RETRY_CONNECTION)
    es.check_indices()


@app.get("/search_ds")
async def search_ds(
    dataset_ids: Optional[List[str]] = Query(None),
    ds_name: str = None,
    ev_name: str = None,
    ev_type: int = None,
    pv_list: List[str] = Query(None),
):
    """
    Search for datasets that contain **at least** the PVs given as a parameter.
    The dataset can contain more PVs than the ones defined, it does not need to be a perfect match.
    All the parameters except the id can contain wildcards, including PV names.

    Arguments:
    - **dataset_ids** (List[str], optional): list of dataset IDs to consider for the search
    - **ds_name** (str, optional): name of the dataset
    - **ev_name** (str, optional): name of the event
    - **ev_type** (int, optional): event type
    - **pv_list** (List[str], optional): list of PVs that should be included in the dataset

    Returns: a list of dataset descriptions
    """
    # Accept also a comma separated list
    if dataset_ids is not None and len(dataset_ids) == 1 and dataset_ids[0].count(","):
        dataset_ids = dataset_ids[0].split(",")
    if pv_list is not None and len(pv_list) == 1 and pv_list[0].count(","):
        pv_list = pv_list[0].split(",")

    dataset_list = es.search_datasets(dataset_ids, ds_name, ev_name, ev_type, pv_list)

    return {"datasets": dataset_list}


@app.get("/search_events")
async def search_events(
    dataset_ids: Optional[List[str]] = Query(None),
    start: Optional[int] = None,
    end: Optional[int] = None,
    tg_pulse_id_start: Optional[int] = None,
    tg_pulse_id_end: Optional[int] = None,
):
    """
    Search for an event in the index.
    - **dataset_ids** (List[str], optional): list of dataset IDs to consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **tg_pulse_id_start** (int, optional): 
    - **tg_pulse_id_end** (int, optional):

    To search for a set of PVs, first one needs to search for datasets containing those PVs and then search by dataset IDs.
    """
    events = es.search_events(
        dataset_ids, start, end, tg_pulse_id_start, tg_pulse_id_end
    )

    return events


@app.get("/get_file")
def get_file(file_path: str):
    """
    Get a NeXus file from the storage
    - **file_path** (str, required): file path
    """

    return FileResponse(
        os.path.join(DATA_STORAGE, file_path),
        filename=os.path.basename(file_path),
        media_type="application/x-hdf5",
    )


from pydantic import BaseModel


class Event(BaseModel):
    datasetId: str
    timestamp: int
    tgPulseId: int
    path: str


@app.post("/get_events")
def get_events(events: List[Event]):
    """
    Get a set of NeXus files containing the requested events, one file per dataset.
    - **events** (List[Event], required): list of events to download
    """

    ## If all the events requested and only those are stored in a single file, return that file.
    paths = list(set([e.path for e in events]))
    if len(paths) == 1:
        response = es.get_file_events(paths[0])
        # If the number of events in the file is the same as the number of events requested...
        # No check is done on the events, assuming they exist an were obtained using `/search_events`
        if len(response) == len(events):
            return FileResponse(
                os.path.join(DATA_STORAGE, paths[0]),
                filename=os.path.basename(paths[0]),
                media_type="application/x-hdf5",
            )

    datasets = list(set([e.datasetId for e in events]))
    pulses_per_dataset = {}
    for dataset in datasets:
        pulses_per_dataset[dataset] = {"pulses": [], "paths": []}
        for event in events:
            if event.datasetId == dataset:
                pulses_per_dataset[dataset]["filename"] = (
                    event.path[:-19] + ".h5"
                )  # removing timestamp
                pulses_per_dataset[dataset]["pulses"].append(str(event.tgPulseId))
                pulses_per_dataset[dataset]["paths"].append(str(event.path))

    # Create a zip file in memory to collect the data before transferring it
    zip_io = BytesIO()
    zip_filename = "events.zip"
    with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
        for dataset in pulses_per_dataset.keys():
            h5_io = BytesIO()
            new_h5_file = MemoryNexus(h5_io)

            for path in pulses_per_dataset[dataset]["paths"]:
                origin_h5_file = hp.File(os.path.join(DATA_STORAGE, path))
                origin_data = origin_h5_file["entry"]["data"]
                pulses_in_file = set.intersection(
                    set(pulses_per_dataset[dataset]["pulses"]), set(origin_data.keys())
                )
                for pulse in pulses_in_file:
                    new_h5_file.copy(origin_data[pulse])
                origin_h5_file.close()

            new_h5_file.close()

            h5_io.seek(0)
            zip.writestr(pulses_per_dataset[dataset]["filename"], h5_io.read())
            h5_io.close()

    return StreamingResponse(
        iter([zip_io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename=%s" % zip_filename},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app, host="0.0.0.0",
    )
