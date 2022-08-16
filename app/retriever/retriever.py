import logging
import os
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
from common import crud, schemas
from common.db.connection import wait_for_connection
from common.files import Collector
from common.models import Dataset
from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from retriever.config import settings

HDF5_MIME_TYPE = "application/x-hdf5"

logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
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
async def query_collectors(
    name: Optional[str] = None,
    event_name: Optional[str] = None,
    event_code: Optional[int] = None,
    pv: Optional[List[str]] = Query(default=None),
):
    """
    Search for collectors that contain **at least** the PVs given as a parameter.
    The collector can contain more PVs than the ones defined, it does not need to be a perfect match.
    All the parameters can contain wildcards, including PV names.

    Arguments:
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
    if event_code is not None:
        filters.append({"term": {"event_code": event_code}})
    if pv:
        filters.append(
            {
                "query_string": {
                    "query": " AND ".join(map(lambda s: s.replace(":", r"\:"), pv)),
                    "default_field": "pvs",
                }
            }
        )
    collectors = await crud.collector.get_multi(filters=filters)
    return collectors


@collectors_router.get("/{id}", response_model=schemas.Collector)
async def get_collector(
    *,
    id: Any,
):
    """
    Return the collector with the given `id`
    """
    collector = await crud.collector.get(id)
    if not collector:
        raise HTTPException(status_code=404, detail="Collector not found")
    return collector


app.include_router(collectors_router, prefix="/collectors", tags=["collectors"])


# Datasets

datasets_router = APIRouter()


@datasets_router.get("", response_model=List[schemas.Dataset])
async def query_datasets(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    trigger_pulse_id_start: Optional[int] = None,
    trigger_pulse_id_end: Optional[int] = None,
) -> List[Dataset]:
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
        timestamp_range = {}
        if start:
            timestamp_range["gte"] = start
        if end:
            timestamp_range["lte"] = end

        filters.append({"range": {"trigger_date": timestamp_range}})
    if trigger_pulse_id_start or trigger_pulse_id_end:
        pulse_id_range = {}
        if trigger_pulse_id_start:
            pulse_id_range["gte"] = trigger_pulse_id_start
        if trigger_pulse_id_end:
            pulse_id_range["lte"] = trigger_pulse_id_end
        filters.append({"range": {"trigger_pulse_id": pulse_id_range}})
    datasets = await crud.dataset.get_multi(filters=filters)
    return datasets


@datasets_router.get("/{id}", response_model=schemas.Dataset)
async def get_dataset(
    *,
    id: Any,
):
    """
    Get the dataset metadata with the give `id`
    """
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


app.include_router(datasets_router, prefix="/datasets", tags=["datasets"])

# Files

files_router = APIRouter()


@files_router.get("", response_class=FileResponse)
async def get_file_by_path(
    *,
    path: Path,
):
    """
    Get a NeXus file from the storage
    - **path** (str, required): file path
    """

    if not (settings.storage_path / path).exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        settings.storage_path / path,
        filename=path.name,
        media_type=HDF5_MIME_TYPE,
    )


@files_router.get("/dataset/{id}", response_class=FileResponse)
async def get_file_by_dataset_id(
    *,
    id: Any,
):
    """
    Get the NeXus file containing the dataset with the given `id`
    """
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    dataset = schemas.Dataset.from_orm(dataset)

    if not (settings.storage_path / dataset.path).exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        settings.storage_path / dataset.path,
        filename=dataset.path.name,
        media_type=HDF5_MIME_TYPE,
    )


@files_router.get("/datasets", response_class=StreamingResponse)
async def get_file_by_dataset_query(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    trigger_pulse_id_start: Optional[int] = None,
    trigger_pulse_id_end: Optional[int] = None,
):
    """
    Search for datasets in the index and returns a file containing all hits.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **trigger_pulse_id_start** (int, optional):
    - **trigger_pulse_id_end** (int, optional):

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    datasets = await query_datasets(
        collector_id, start, end, trigger_pulse_id_start, trigger_pulse_id_end
    )
    if datasets == []:
        raise HTTPException(status_code=404, detail="Datasets not found")

    return await get_file_with_multiple_datasets(
        [schemas.DatasetBase.parse_obj(dataset) for dataset in datasets]
    )


@files_router.post("/compile", response_class=StreamingResponse)
async def get_file_with_multiple_datasets(datasets: List[schemas.DatasetBase]):
    """
    Get a set of NeXus files containing the requested datasets, one file per collector (zipped if needed).
    - **datasets** (List[Dataset], required): list of datasets to download
    """

    # If all the datasets requested and only those are stored in a single file, return that file.
    paths = list(set([ds.path for ds in datasets]))
    if len(paths) == 1:
        response = crud.dataset.get_multi_by_path(paths[0])
        # If the number of datasets in the file is the same as the number of datasets requested...
        # No check is done on the datasets, assuming they exist an were obtained using the `/datasets` endpoint
        if len(response) == len(datasets):
            return FileResponse(
                settings.storage_path / paths[0],
                filename=paths[0].name,
                media_type=HDF5_MIME_TYPE,
            )

    collectors: Dict[str, Collector] = dict()
    for dataset in datasets:
        collector = collectors.get(dataset.collector_id)
        if collector is None:
            collectors[dataset.collector_id] = Collector.create(dataset)
        else:
            collector.update(dataset)

    # Create a temporary zip file to collect the data before transferring it
    async with aiofiles.tempfile.TemporaryDirectory() as d:
        zip_filename = "datasets.zip"
        zip_io = BytesIO()
        with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
            for collector in collectors.values():
                await collector.write(d)

                zip.write(os.path.join(d, collector.path), collector.path)

    return StreamingResponse(
        iter([zip_io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )


app.include_router(files_router, prefix="/files", tags=["files"])
