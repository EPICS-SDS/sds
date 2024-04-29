import asyncio
import logging
from threading import Lock
from typing import Optional

from fastapi import APIRouter, Response, status

from esds.common import crud, schemas
from esds.common.db.connection import wait_for_connection
from esds.common.db.utils import dict_to_filters
from esds.common.fast_api_offline import FastAPIOfflineDocs
from esds.indexer.config import settings
from esds.indexer.init_db import init_db

ch = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
ch.setFormatter(formatter)
ch.setLevel(settings.log_level)
logging.getLogger().addHandler(ch)
logging.getLogger().setLevel(settings.log_level)

logger = logging.getLogger(__name__)

description = """
This API can be used for:
- add/update collector definitions
- add new datasets for indexing
"""
app = FastAPIOfflineDocs(
    doc_cdon_files="static",
    title="SDS Indexer Service API",
    description=description,
    version="0.1",
)


@app.on_event("startup")
async def startup_event():
    await wait_for_connection()
    await init_db()


# Collectors

collectors_router = APIRouter()
collectors_lock = Lock()
requested_collectors = []


@collectors_router.post(
    "",
    response_model=schemas.Collector,
    status_code=status.HTTP_201_CREATED,
)
async def create_collector(
    *,
    response: Response,
    collector_in: schemas.CollectorCreate,
):
    # Make sure the creation of a collector is an atomic operation.
    while True:
        with collectors_lock:
            if collector_in.name not in requested_collectors:
                requested_collectors.append(collector_in.name)
                break
        await asyncio.sleep(0)

    filters = dict_to_filters(collector_in.model_dump(exclude={"created"}))

    (_, collectors, _) = await crud.collector.get_multi(filters=filters)
    # HTTP 200 if the collector already exists
    if len(collectors) > 0:
        response.status_code = status.HTTP_200_OK
        collector = collectors[0]
    else:
        collector = await crud.collector.create(obj_in=collector_in)
        await crud.collector.refresh_index()

    with collectors_lock:
        requested_collectors.remove(collector_in.name)

    return collector


app.include_router(collectors_router, prefix="/collectors", tags=["Collectors"])


# Datasets

datasets_router = APIRouter()


@datasets_router.post(
    "",
    response_model=schemas.Dataset,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    *,
    ttl: Optional[int] = None,
    dataset_in: schemas.DatasetCreate,
):
    """
    Create a dataset
    - **ttl**: time for the dataset to live in seconds
    """
    dataset = await crud.dataset.create(dataset_in, ttl=ttl)
    return dataset


app.include_router(datasets_router, prefix="/datasets", tags=["Datasets"])


status_router = APIRouter()


@status_router.get("", status_code=status.HTTP_200_OK)
def healthcheck():
    """
    Simple health check that returns 200 OK when service is running.
    """
    return {"health": "Everything OK!"}


app.include_router(status_router, prefix="/health", tags=["Status"])
