import logging
import os
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from h5py import File

from esds.common import crud, schemas
from esds.common.db import settings as db_settings
from esds.common.db.connection import wait_for_connection
from esds.common.db.utils import dict_to_filters
from esds.common.fast_api_offline import FastAPIOfflineDocs
from esds.common.files import NexusFile
from esds.common.files.json_file import JsonFile, numpy_encoder
from esds.retriever.config import settings
from esds.retriever.schemas import MultiResponseCollector, MultiResponseDataset

ch = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
ch.setFormatter(formatter)
ch.setLevel(settings.log_level)
logging.getLogger().addHandler(ch)
logging.getLogger().setLevel(settings.log_level)

logger = logging.getLogger(__name__)

HDF5_MIME_TYPE = "application/x-hdf5"


@asynccontextmanager
async def lifespan(app: FastAPIOfflineDocs):
    # Wait for elasticsearch server to be available
    await wait_for_connection()
    yield


description = """
This API can be used for:
- get collectors configuration by query or by ID
- get datasets by a search query or by ID
- get files by path, search query over datasets, by ID, or by a subset/combination of results from a dataset query
- get data as json by path, search query over datasets, by ID, or by a subset/combination of results from a dataset query
"""

app = FastAPIOfflineDocs(
    doc_cdon_files="static",
    title="SDS Retriever Service API",
    description=description,
    version="0.2",
    lifespan=lifespan,
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


# Collectors

collectors_router = APIRouter()


@collectors_router.get("", response_model=MultiResponseCollector)
async def query_collectors(
    name: Optional[str] = None,
    parent_path: Optional[str] = None,
    event_code: Optional[int] = None,
    pv: Optional[List[str]] = Query(default=None),
    sort: Optional[SortOrder] = SortOrder.desc,
    latest_version_only: Optional[bool] = Query(
        description="Return only the latest version of each collector", default=True
    ),
    search_after: Optional[int] = None,
):
    """
    Search for collectors that contain **at least** the PVs given as a parameter.
    The collector can contain more PVs than the ones defined, it does not need to be a perfect match.
    All the parameters can contain wildcards, including PV names.

    Arguments:
    - **name** (str, optional): name of the collector
    - **parent_path** (str, optional): parent_path of the collector
    - **event_code** (int, optional): event code
    - **pv** (List[str], optional): list of PVs

    Returns: a list of dataset descriptions
    """
    filters = []
    if name is not None:
        filters.append({"wildcard": {"name": name}})
    if parent_path is not None:
        filters.append({"wildcard": {"parent_path": parent_path}})
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

    if latest_version_only:
        aggs = {
            "latest_version": {
                "terms": {
                    "field": "collector_id",
                    "size": db_settings.max_query_size,
                    "order": {"max_version": "desc"},
                },
                "aggs": {
                    "max_version": {"max": {"field": "version"}},
                    "latest": {"top_hits": {"size": 1, "sort": sort}},
                },
            }
        }
        size = 0
    else:
        aggs = None
        size = None

    total, collectors, search_after = await crud.collector.get_multi(
        filters=filters, aggs=aggs, sort=sort, search_after=search_after, size=size
    )

    return MultiResponseCollector(
        total=total, collectors=collectors, search_after=search_after
    )


@collectors_router.get("/{collector_id}", response_model=MultiResponseCollector)
async def get_collector(
    *,
    collector_id: Any,
):
    """
    Return the collector with the given `collector_id`, as a list with all the versions available.
    """
    sort = {"created": {"order": SortOrder.desc}}
    filters = dict_to_filters({"collector_id": collector_id})

    total, collectors, search_after = await crud.collector.get_multi(
        filters=filters, sort=sort
    )
    if total == 0:
        raise HTTPException(status_code=404, detail="Collector not found")

    return MultiResponseCollector(
        total=total, collectors=collectors, search_after=search_after
    )


app.include_router(collectors_router, prefix="/collectors", tags=["collectors"])


# Datasets

datasets_router = APIRouter()


@datasets_router.get("", response_model=MultiResponseDataset)
async def query_datasets(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_cycle_id_start: Optional[int] = None,
    sds_event_cycle_id_end: Optional[int] = None,
    size: Optional[int] = None,
    sort: Optional[SortOrder] = SortOrder.desc,
    search_after: Optional[int] = None,
):
    """
    Search for datasets in the index.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_cycle_id_start** (int, optional): SDS event cycle ID for interval start
    - **sds_event_cycle_id_end** (int, optional): SDS event cycle ID for interval end
    - **size** (int, optional): Limit the number of results. If used alone, it will return the latest *size* datasets
    - **sort** (SortOrder, optional): to sort results in ascending or descending order in time (descending by default)
    - **search_after** (int, optional): to scroll over a large number of hits

    To search for a set of PVs, first one needs to search for collectors
    containing those PVs and then search by collector IDs.
    """
    filters = []
    if collector_id is not None:
        collectors_filter = []
        for id in collector_id:
            collectors_filter.append({"match": {"collector_id": id}})
        filters.append({"bool": {"should": collectors_filter}})
    if start is not None or end is not None:
        timestamp_range = {}
        if start:
            timestamp_range["gte"] = start
        if end:
            timestamp_range["lte"] = end

        filters.append({"range": {"sds_event_timestamp": timestamp_range}})
    if sds_event_cycle_id_start is not None or sds_event_cycle_id_end is not None:
        cycle_id_range = {}
        if sds_event_cycle_id_start is not None:
            cycle_id_range["gte"] = sds_event_cycle_id_start
        if sds_event_cycle_id_end is not None:
            cycle_id_range["lte"] = sds_event_cycle_id_end
        filters.append({"range": {"sds_event_cycle_id": cycle_id_range}})

    if size is not None:
        if size < 0 or size > db_settings.max_query_size:
            raise HTTPException(
                status_code=400,
                detail=f"Query with 'size' parameter must be positive and smaller than {db_settings.max_query_size}.",
            )

    sort = {"sds_event_timestamp": {"order": sort.value}}

    total, datasets, search_after = await crud.dataset.get_multi(
        filters=filters, sort=sort, search_after=search_after, size=size
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
async def get_nexus_by_path(
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
async def get_nexus_by_dataset_id(
    *,
    id: Any,
):
    """
    Get the NeXus file containing the dataset with the given `id`
    """
    dataset = await crud.dataset.get(id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    dataset = schemas.Dataset.model_validate(dataset)

    if not (settings.storage_path / dataset.path).exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        settings.storage_path / dataset.path,
        filename=dataset.path.name,
        media_type=HDF5_MIME_TYPE,
    )


@files_router.get("/query", response_class=StreamingResponse)
async def get_nexus_by_dataset_query(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_cycle_id_start: Optional[int] = None,
    sds_event_cycle_id_end: Optional[int] = None,
    size: Optional[int] = None,
    include_pvs: Optional[List[str]] = Query(default=None),
):
    """
    Search for datasets in the index and returns a file containing all hits.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_cycle_id_start** (int, optional): SDS event cycle ID for interval start
    - **sds_event_cycle_id_end** (int, optional): SDS event cycle ID for interval end
    - **size** (int, optional): Limit the number of results. If used alone, it will return the latest *size* datasets
    - **search_after** (int, optional): to scroll over a large number of hits
    - **include_pvs** (List[str], optional): list of PVs to return

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
            sds_event_cycle_id_start,
            sds_event_cycle_id_end,
            search_after=search_after,
            size=size,
        )

        if dataset_respone.datasets == []:
            break
        else:
            datasets.extend(dataset_respone.datasets)
            search_after = dataset_respone.search_after

    if datasets == []:
        raise HTTPException(status_code=404, detail="Datasets not found")

    return await get_nexus_with_multiple_datasets(
        [schemas.DataseDefinition.model_validate(dataset) for dataset in datasets],
        include_pvs=include_pvs,
    )


@files_router.post("/datasets", response_class=StreamingResponse)
async def get_nexus_with_multiple_datasets(
    datasets: List[schemas.DataseDefinition],
    include_pvs: Optional[List[str]] = Query(default=None),
):
    """
    Get a set of NeXus files containing the requested datasets, one file per collector (zipped if needed).
    - **datasets** (List[Dataset], required): list of datasets to download
    - **include_pvs** (List[str], optional): list of PVs to return
    """

    # If all the datasets requested and only those are stored in a single file, return that file.
    paths = list(set([ds.path for ds in datasets]))
    if len(paths) == 1:
        total, response, _ = await crud.dataset.get_multi_by_path(paths[0])
        # If the number of datasets in the file is the same as the number of datasets requested...
        # No check is done on the datasets, assuming they exist an were obtained using the `/datasets` endpoint
        if total == len(datasets) and include_pvs is None:
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
                parent_path = origin["entry"].attrs["collector_parent_path"]
                origin.close()

                # First create a new file
                nexus_file = NexusFile(
                    collector_id=dataset.collector_id,
                    collector_name=collector_name,
                    parent_path=parent_path,
                    file_name=collector_name + ".h5",
                    directory=Path(d),
                )
                nexus_files[dataset.collector_id] = nexus_file

            nexus_file.add_dataset(dataset)

        zip_filename = "datasets.zip"
        zip_io = BytesIO()
        with zipfile.ZipFile(zip_io, mode="w", compression=zipfile.ZIP_DEFLATED) as zip:
            for nexus_file in nexus_files.values():
                nexus_file.write_from_datasets(include_pvs=include_pvs)
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


@json_router.get("/query", response_class=JSONResponse)
async def get_json_by_dataset_query(
    collector_id: Optional[List[str]] = Query(default=None),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    sds_event_cycle_id_start: Optional[int] = None,
    sds_event_cycle_id_end: Optional[int] = None,
    size: Optional[int] = None,
    include_pvs: Optional[List[str]] = Query(default=None),
):
    """
    Search for datasets in the index and returns a json with all hits.
    - **collector_id** (List[str], optional): list of collector IDs to
      consider for the search
    - **start** (int, optional): UTC timestamp for interval start
    - **end** (int, optional): UTC timestamp for interval end
    - **sds_event_cycle_id_start** (int, optional): SDS event cycle ID for interval start
    - **sds_event_cycle_id_end** (int, optional): SDS event cycle ID for interval end
    - **size** (int, optional): Limit the number of results. If used alone, it will return the latest *size* datasets
    - **include_pvs** (List[str], optional): list of PVs to return

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
            sds_event_cycle_id_start,
            sds_event_cycle_id_end,
            search_after=search_after,
            size=size,
        )

        if dataset_respone.datasets == []:
            break
        else:
            datasets.extend(dataset_respone.datasets)
            search_after = dataset_respone.search_after

    if datasets == []:
        raise HTTPException(status_code=404, detail="Datasets not found")

    return await get_json_with_multiple_datasets(
        datasets=[
            schemas.DataseDefinition.model_validate(dataset) for dataset in datasets
        ],
        include_pvs=include_pvs,
    )


@json_router.post("/datasets", response_class=JSONResponse)
async def get_json_with_multiple_datasets(
    datasets: List[schemas.DataseDefinition],
    include_pvs: Optional[List[str]] = Query(default=None),
):
    """
    Get a Json containing the requested datasets.
    - **datasets** (List[Dataset], required): list of datasets to download
    - **include_pvs** (List[str], optional): list of PVs to return
    """
    collectors_json: Dict[str, Any] = dict()

    # Create a temporary zip file to collect the data before transferring it
    for dataset in datasets:
        dataset_json = collectors_json.get(dataset.collector_id)

        if dataset_json is None:
            dataset_json = JsonFile(pvs=include_pvs)
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
