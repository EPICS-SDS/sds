from io import BytesIO
from typing import Any, List, Optional

import logging
from datetime import datetime
from pathlib import Path
import zipfile
from fastapi import FastAPI, APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from common import crud, schemas
from common.db.connection import wait_for_connection
from retriever.config import settings
from retriever.memory_nexus import MemoryNexus


logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(settings.log_level)

app = FastAPI(title="SDS Retriever")


@app.on_event("startup")
async def startup_event():
    await wait_for_connection()


# Collectors

collectors_router = APIRouter()


@collectors_router.get("", response_model=List[schemas.Collector])
async def read_collectors(
    name: Optional[str] = None,
    event_name: Optional[str] = None,
    event_code: Optional[int] = None,
    pv: Optional[List[str]] = Query(default=None),
):
    """
    Search for collectors that contain **at least** the PVs given as a parameter.
    The collector can contain more PVs than the ones defined, it does not need to be a perfect match.
    All the parameters except the id can contain wildcards, including PV names.

    Arguments:
    - **id** (List[str], optional): list of collector IDs
    - **name** (str, optional): name of the collector
    - **event_name** (str, optional): name of the event
    - **event_code** (int, optional): event code
    - **pv** (List[str], optional): list of PVs

    Returns: a list of dataset descriptions
    """
    filters = []
    if name:
        filters.append({"wildcard": {"name": name}})
    if event_name:
        filters.append({"wildcard": {"event_name": event_name}})
    if event_code:
        filters.append({"wildcard": {"event_code": event_code}})
    if pv:
        filters.append({"query_string": {
            "query": " ".join(map(lambda s: s.replace(":", r"\:"), pv)),
            "default_field": "pvs",
            "default_operator": "AND",
        }})
    collectors = await crud.collector.get_multi(filters=filters)
    return collectors


@collectors_router.get("/{id}", response_model=schemas.Collector)
async def read_collector(
    *,
    id: Any,
):
    collector = await crud.collector.get(id)
    if not collector:
        raise HTTPException(status_code=404, detail="Collector not found")
    return collector

app.include_router(collectors_router, prefix="/collectors",
                   tags=["collectors"])


# Datasets

datasets_router = APIRouter()


@datasets_router.get("", response_model=List[schemas.Dataset])
async def read_datasets(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    trigger_pulse_id_start: Optional[int] = None,
    trigger_pulse_id_end: Optional[int] = None,
):
    """
    Search for datasets in the index.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **trigger_pulse_id_start** (int, optional):
    - **trigger_pulse_id_end** (int, optional):

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    filters = []
    if collector_id:
        for id in collector_id:
            filters.append({"match": {"collector_id": id}})
    if start or end:
        range = {}
        if start:
            range["gte"] = start
        if end:
            range["lte"] = end
        filters.append({"range": {"created": range}})
    if trigger_pulse_id_start or trigger_pulse_id_end:
        range = {}
        if trigger_pulse_id_start:
            range["gte"] = trigger_pulse_id_start
        if trigger_pulse_id_end:
            range["lte"] = trigger_pulse_id_end
        filters.append({"range": {"trigger_pulse_id": range}})
    datasets = await crud.dataset.get_multi(filters=filters)
    return datasets


@datasets_router.get("/{id}", response_model=schemas.Dataset)
async def read_dataset(
    *,
    id: Any,
):
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@datasets_router.get("/{id}/file", response_class=FileResponse)
async def read_dataset_file(
    *,
    id: Any,
):
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    dataset = schemas.Dataset.from_orm(dataset)

    return FileResponse(
        settings.storage_path / dataset.path,
        filename=dataset.path.name,
        media_type="application/x-hdf5",
    )


app.include_router(datasets_router, prefix="/datasets", tags=["datasets"])

# Files

files_router = APIRouter()


@files_router.get("", response_class=FileResponse)
async def read_file(
    *,
    path: Path,
):
    """
    Get a NeXus file from the storage
    - **path** (str, required): file path
    """
    return FileResponse(
        settings.storage_path / path,
        filename=path.name,
        media_type="application/x-hdf5",
    )

app.include_router(files_router, prefix="/files", tags=["datasets"])


@files_router.post("/compile", response_class=StreamingResponse)
def get_datasets(datasets: List[schemas.DatasetCreate]):
    """
    Get a set of NeXus files containing the requested datasets, one file per collector.
    - **datasets** (List[Dataset], required): list of datasets to download
    """

    # If all the datasets requested and only those are stored in a single file, return that file.
    paths = list(set([ds.path for ds in datasets]))
    if len(paths) == 1:
        response = crud.dataset.get_multi_by_path(paths[0])
        # If the number of datasets in the file is the same as the number of datasets requested...
        # No check is done on the datasets, assuming they exist an were obtained using `/search_datasets`
        if len(response) == len(datasets):
            return FileResponse(
                settings.storage_path / paths[0],
                filename=paths[0].name,
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
                origin_h5_file = hp.File(settings.storage_path / path)
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
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )