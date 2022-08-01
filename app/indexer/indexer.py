from typing import Optional

import logging
from fastapi import FastAPI, APIRouter, Response, status

from common import crud, schemas
from common.db.connection import wait_for_connection
from common.db.utils import dict_to_filters
from indexer.config import settings
from indexer.init_db import init_db


logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(settings.log_level)

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    await wait_for_connection()
    await init_db()


# Collectors

collectors_router = APIRouter()


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
    collectors = await crud.collector.get_multi(filters=filters)
    # HTTP 200 if the collector already exists
    if len(collectors) > 0:
        response.status_code = status.HTTP_200_OK
        return collectors[0]
    collector = await crud.collector.create(obj_in=collector_in)
    return collector


app.include_router(collectors_router, prefix="/collectors", tags=["collectors"])


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
    dataset = await crud.dataset.create(obj_in=dataset_in, ttl=ttl)
    return dataset


app.include_router(datasets_router, prefix="/datasets", tags=["datasets"])
