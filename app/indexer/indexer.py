import asyncio
import logging
from multiprocessing import Lock
from typing import Optional

from common import crud, schemas
from common.db.connection import wait_for_connection
from common.db.utils import dict_to_filters
from fastapi import APIRouter, FastAPI, Response, status

from indexer.config import settings
from indexer.init_db import init_db

logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(settings.log_level)

description = """
This API can be used for:
- add/update collector definitions
- add new datasets for indexing
"""
app = FastAPI(
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
    filters = dict_to_filters(collector_in.dict(exclude={"created"}))
    # Workaround to acquire a multithreading lock without blocking the event loop
    while not collectors_lock.acquire(block=False):
        await asyncio.sleep(0)

    (_, collectors, _) = await crud.collector.get_multi(filters=filters)
    # HTTP 200 if the collector already exists
    if len(collectors) > 0:
        response.status_code = status.HTTP_200_OK
        collectors_lock.release()
        return collectors[0]

    collector = await crud.collector.create(obj_in=collector_in)
    await crud.collector.refresh_index()

    collectors_lock.release()
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
