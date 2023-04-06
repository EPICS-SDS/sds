import logging
import os
import zipfile
from datetime import datetime
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
from esds.common import crud, schemas
from esds.common.db.connection import wait_for_connection
from esds.common.files import NexusFile
from esds.common.files.json_file import JsonFile, numpy_encoder
from esds.retriever.config import settings
from esds.retriever.schemas import MultiResponseCollector, MultiResponseDataset
from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from h5py import File

HDF5_MIME_TYPE = "application/x-hdf5"

logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(settings.log_level)

description = """
This API can be used for:
- get collectors configuration by query or by ID
- get datasets by a search query or by ID
- get files by path, search query over datasets, by ID, or by a subset/combination of results from a dataset query
- get data as json by path, search query over datasets, by ID, or by a subset/combination of results from a dataset query
"""
app = FastAPI(
    title="SDS Retriever Service API",
    description=description,
    version="0.1",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SortOrder(str, Enum):
    desc = "desc"
    asc = "asc"


@app.on_event("startup")
async def startup_event():
    await wait_for_connection()


# Collectors

collectors_router = APIRouter()


@collectors_router.get("", response_model=MultiResponseCollector)
async def query_collectors(
    name: Optional[str] = None,
    event_name: Optional[str] = None,
    event_code: Optional[int] = None,
    pv: Optional[List[str]] = Query(default=None),
    sort: Optional[SortOrder] = SortOrder.desc,
    search_after: Optional[int] = None,
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
    if name is not None:
        filters.append({"wildcard": {"name": name}})
    if event_name is not None:
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
    sort = {"created": {"order": sort}}

    total, collectors, search_after = await crud.collector.get_multi(
        filters=filters, sort=sort, search_after=search_after
    )

    return MultiResponseCollector(
        total=total, collectors=collectors, search_after=search_after
    )


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


@datasets_router.get("", response_model=MultiResponseDataset)
async def query_datasets(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_pulse_id_start: Optional[int] = None,
    sds_event_pulse_id_end: Optional[int] = None,
    sort: Optional[SortOrder] = SortOrder.desc,
    search_after: Optional[int] = None,
):
    """
    Search for datasets in the index.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_pulse_id_start** (int, optional): SDS event pulse ID for interval start
    - **sds_event_pulse_id_end** (int, optional): SDS event pulse ID for interval end
    - **sort** (SortOrder, optional): to sort results in ascending or descending order in time
    - **search_after** (int, optional): to scroll over a large number of hits

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    filters = []
    if collector_id is not None:
        for id in collector_id:
            filters.append({"match": {"collector_id": id}})
    if start is not None or end is not None:
        timestamp_range = {}
        if start:
            timestamp_range["gte"] = start
        if end:
            timestamp_range["lte"] = end

        filters.append({"range": {"sds_event_timestamp": timestamp_range}})
    if sds_event_pulse_id_start is not None or sds_event_pulse_id_end is not None:
        pulse_id_range = {}
        if sds_event_pulse_id_start is not None:
            pulse_id_range["gte"] = sds_event_pulse_id_start
        if sds_event_pulse_id_end is not None:
            pulse_id_range["lte"] = sds_event_pulse_id_end
        filters.append({"range": {"sds_event_pulse_id": pulse_id_range}})

    sort = {"sds_event_timestamp": {"order": sort.value}}

    total, datasets, search_after = await crud.dataset.get_multi(
        filters=filters, sort=sort, search_after=search_after
    )

    return MultiResponseDataset(
        total=total, datasets=datasets, search_after=search_after
    )


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


@files_router.get("/", response_class=FileResponse)
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


@files_router.get("/search", response_class=StreamingResponse)
async def get_file_by_dataset_query(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_pulse_id_start: Optional[int] = None,
    sds_event_pulse_id_end: Optional[int] = None,
):
    """
    Search for datasets in the index and returns a file containing all hits.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_pulse_id_start** (int, optional): SDS event pulse ID for interval start
    - **sds_event_pulse_id_end** (int, optional): SDS event pulse ID for interval end
    - **search_after** (int, optional): to scroll over a large number of hits

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    datasets: List[schemas.DatasetBase] = []
    search_after = None
    while True:
        dataset_respone = await query_datasets(
            collector_id,
            start,
            end,
            sds_event_pulse_id_start,
            sds_event_pulse_id_end,
            search_after=search_after,
        )

        if dataset_respone.datasets == []:
            break
        else:
            datasets.extend(dataset_respone.datasets)
            search_after = dataset_respone.search_after

    if datasets == []:
        raise HTTPException(status_code=404, detail="Datasets not found")

    return await get_file_with_multiple_datasets(
        [schemas.DataseDefinition.parse_obj(dataset) for dataset in datasets]
    )


@files_router.post("/datasets", response_class=StreamingResponse)
async def get_file_with_multiple_datasets(datasets: List[schemas.DataseDefinition]):
    """
    Get a set of NeXus files containing the requested datasets, one file per collector (zipped if needed).
    - **datasets** (List[Dataset], required): list of datasets to download
    """

    # If all the datasets requested and only those are stored in a single file, return that file.
    paths = list(set([ds.path for ds in datasets]))
    if len(paths) == 1:
        total, response, _ = await crud.dataset.get_multi_by_path(paths[0])
        # If the number of datasets in the file is the same as the number of datasets requested...
        # No check is done on the datasets, assuming they exist an were obtained using the `/datasets` endpoint
        if total == len(datasets):
            return FileResponse(
                settings.storage_path / paths[0],
                filename=paths[0].name,
                media_type=HDF5_MIME_TYPE,
            )

    nexus_files: Dict[str, NexusFile] = dict()

    # Create a temporary zip file to collect the data before transferring it
    async with aiofiles.tempfile.TemporaryDirectory() as d:
        for dataset in datasets:
            nexus_file = nexus_files.get(dataset.collector_id)

            if nexus_file is None:
                origin = File(settings.storage_path / dataset.path, "r")
                collector_name = origin["entry"].attrs["collector_name"]
                origin.close()

                # First create a new file
                nexus_file = NexusFile(
                    collector_id=dataset.collector_id,
                    collector_name=collector_name,
                    file_name=collector_name + ".h5",
                    directory=Path(d),
                )
                nexus_files[dataset.collector_id] = nexus_file

            nexus_file.add_dataset(dataset)

        zip_filename = "datasets.zip"
        zip_io = BytesIO()
        with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
            for nexus_file in nexus_files.values():
                nexus_file.write_from_datasets()
                zip.write(os.path.join(d, nexus_file.path), nexus_file.file_name)

    return StreamingResponse(
        iter([zip_io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )


app.include_router(files_router, prefix="/nexus", tags=["nexus"])


# Json
json_router = APIRouter()


@json_router.get("", response_class=JSONResponse)
async def get_json_by_path(
    *,
    path: Path,
    pvs: Optional[List[str]] = Query(default=None),
):
    """
    Get a json representation of a file from the storage
    - **path** (str, required): file path
    - **pvs** (List[str], optional): list of PVs to return
    """

    if not (settings.storage_path / path).exists():
        raise HTTPException(status_code=404, detail="File not found")
    _, datasets, _ = await crud.dataset.get_multi_by_path(path)

    dataset_json = JsonFile(pvs=pvs)

    for dataset in datasets:
        dataset_json.add_dataset(dataset)

    return JSONResponse(
        content=jsonable_encoder(dataset_json.json(), custom_encoder=numpy_encoder)
    )


@json_router.get("/dataset/{id}", response_class=JSONResponse)
async def get_json_by_dataset_id(
    *,
    id: Any,
    pvs: Optional[List[str]] = Query(default=None),
):
    """
    Get the dataset with the given `id` as json
    - **pvs** (List[str], optional): list of PVs to return
    """
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    dataset = schemas.Dataset.from_orm(dataset)

    if not (settings.storage_path / dataset.path).exists():
        raise HTTPException(status_code=404, detail="File not found")

    dataset_json = JsonFile(pvs=pvs)
    dataset_json.add_dataset(dataset)

    return JSONResponse(
        content=jsonable_encoder(dataset_json.json(), custom_encoder=numpy_encoder)
    )


@json_router.get("/search", response_class=JSONResponse)
async def get_json_by_dataset_query(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_pulse_id_start: Optional[int] = None,
    sds_event_pulse_id_end: Optional[int] = None,
    pvs: Optional[List[str]] = Query(default=None),
):
    """
    Search for datasets in the index and returns a json with all hits.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_pulse_id_start** (int, optional): SDS event pulse ID for interval start
    - **sds_event_pulse_id_end** (int, optional): SDS event pulse ID for interval end
    - **search_after** (int, optional): to scroll over a large number of hits
    - **pvs** (List[str], optional): list of PVs to return

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    datasets: List[schemas.DatasetBase] = []
    search_after = None
    while True:
        dataset_respone = await query_datasets(
            collector_id,
            start,
            end,
            sds_event_pulse_id_start,
            sds_event_pulse_id_end,
            search_after=search_after,
        )

        if dataset_respone.datasets == []:
            break
        else:
            datasets.extend(dataset_respone.datasets)
            search_after = dataset_respone.search_after

    if datasets == []:
        raise HTTPException(status_code=404, detail="Datasets not found")

    return await get_json_with_multiple_datasets(
        datasets=[schemas.DataseDefinition.parse_obj(dataset) for dataset in datasets],
        pvs=pvs,
    )


@json_router.post("/datasets", response_class=JSONResponse)
async def get_json_with_multiple_datasets(
    datasets: List[schemas.DataseDefinition],
    pvs: Optional[List[str]] = Query(default=None),
):
    """
    Get a Json containing the requested datasets.
    - **datasets** (List[Dataset], required): list of datasets to download
    - **pvs** (List[str], optional): list of PVs to return
    """
    collectors_json: Dict[str, Any] = dict()

    # Create a temporary zip file to collect the data before transferring it
    for dataset in datasets:
        dataset_json = collectors_json.get(dataset.collector_id)

        if dataset_json is None:
            dataset_json = JsonFile(pvs=pvs)
            collectors_json[dataset.collector_id] = dataset_json

        dataset_json.add_dataset(dataset)

    return JSONResponse(
        content=jsonable_encoder(
            collectors_json,
            custom_encoder={
                **numpy_encoder,
                JsonFile: lambda f: jsonable_encoder(
                    f.json(), custom_encoder=numpy_encoder
                ),
            },
        )
    )


app.include_router(json_router, prefix="/json", tags=["json"])
