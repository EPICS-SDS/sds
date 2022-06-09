import os
import zipfile
from io import BytesIO
from typing import List, Optional

import h5py as hp
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from memory_nexus import MemoryNexus

## To be removed when proper package is published to artifactory
import sys
sys.path.append("../")

from common.elastic import ElasticClient

# Approx. 2 minutes wait for startup in case elastic is starting in parallel
RETRY_CONNECTION = 120

DATA_STORAGE = os.environ.get("DATA_STORAGE", ".")

app = FastAPI()

es = ElasticClient()


@app.on_event("startup")
async def startup_event():
    es.wait_for_connection(RETRY_CONNECTION)
    es.check_indices()


@app.get("/search_collectors")
async def search_collectors(
    collector_ids: Optional[List[str]] = Query(None),
    collector_name: str = None,
    event_name: str = None,
    event_code: int = None,
    pv_list: List[str] = Query(None),
):
    """
    Search for collectors that contain **at least** the PVs given as a parameter.
    The collector can contain more PVs than the ones defined, it does not need to be a perfect match.
    All the parameters except the id can contain wildcards, including PV names.

    Arguments:
    - **collector_ids** (List[str], optional): list of collector IDs to consider for the search
    - **collector** (str, optional): name of the collector
    - **event_name** (str, optional): name of the event
    - **event_code** (int, optional): event code
    - **pv_list** (List[str], optional): list of PVs that should be included in the dataset

    Returns: a list of dataset descriptions
    """
    # Accept also a comma separated list
    if collector_ids is not None and len(collector_ids) == 1 and collector_ids[0].count(","):
        collector_ids = collector_ids[0].split(",")
    if pv_list is not None and len(pv_list) == 1 and pv_list[0].count(","):
        pv_list = pv_list[0].split(",")

    collector_list = es.search_collectors(collector_ids, collector_name, event_name, event_code, pv_list)

    return {"collectors": collector_list}


@app.get("/search_datasets")
async def search_datasets(
    collector_ids: Optional[List[str]] = Query(None),
    start: Optional[int] = None,
    end: Optional[int] = None,
    trigger_pulse_id_start: Optional[int] = None,
    trigger_pulse_id_end: Optional[int] = None,
):
    """
    Search for datasets in the index.
    - **collector_ids** (List[str], optional): list of collector IDs to consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **trigger_pulse_id_start** (int, optional): 
    - **trigger_pulse_id_end** (int, optional):

    To search for a set of PVs, first one needs to search for collectors containing those PVs and then search by collector IDs.
    """
    datasets = es.search_datasets(
        collector_ids, start, end, trigger_pulse_id_start, trigger_pulse_id_end
    )

    return datasets


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


class Dataset(BaseModel):
    collectorId: str
    timestamp: int
    trigger_pulse_id: int
    path: str


@app.post("/get_datasets")
def get_datasets(datasets: List[Dataset]):
    """
    Get a set of NeXus files containing the requested datasets, one file per collector.
    - **datasets** (List[Dataset], required): list of datasets to download
    """

    ## If all the datasets requested and only those are stored in a single file, return that file.
    paths = list(set([ds.path for ds in datasets]))
    if len(paths) == 1:
        response = es.get_file(paths[0])
        # If the number of datasets in the file is the same as the number of datasets requested...
        # No check is done on the datasets, assuming they exist an were obtained using `/search_datasets`
        if len(response) == len(datasets):
            return FileResponse(
                os.path.join(DATA_STORAGE, paths[0]),
                filename=os.path.basename(paths[0]),
                media_type="application/x-hdf5",
            )

    collectors = list(set([ds.collectorId for ds in datasets]))
    pulses_per_collector = {}
    for collector in collectors:
        pulses_per_collector[collector] = {"pulses": [], "paths": []}
        for dataset in datasets:
            if dataset.collectorId == collector:
                pulses_per_collector[collector]["filename"] = (
                    dataset.path[:-19] + ".h5"
                )  # removing timestamp
                pulses_per_collector[collector]["pulses"].append(str(dataset.trigger_pulse_id))
                pulses_per_collector[collector]["paths"].append(str(dataset.path))

    # Create a zip file in memory to collect the data before transferring it
    zip_io = BytesIO()
    zip_filename = "datasets.zip"
    with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
        for collector in pulses_per_collector.keys():
            h5_io = BytesIO()
            new_h5_file = MemoryNexus(h5_io)

            for path in list(set(pulses_per_collector[collector]["paths"])):
                origin_h5_file = hp.File(os.path.join(DATA_STORAGE, path))
                origin_data = origin_h5_file["entry"]["data"]
                pulses_in_file = set.intersection(
                    set(pulses_per_collector[collector]["pulses"]), set(origin_data.keys())
                )
                for pulse in pulses_in_file:
                    new_h5_file.copy(origin_data[pulse])
                origin_h5_file.close()

            new_h5_file.close()

            h5_io.seek(0)
            zip.writestr(pulses_per_collector[collector]["filename"], h5_io.read())
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
