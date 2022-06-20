from typing import Optional

from fastapi import FastAPI, APIRouter, HTTPException, status

from common import crud, schemas
from common.db.connection import wait_for_connection
from init_db import init_db


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
    collector_in: schemas.CollectorCreate
):
    collectors = await crud.collector.get_multi(filters=collector_in.dict())
    if len(collectors) > 0:
        raise HTTPException(
            status_code=400,
            detail="Collector with these properties already exists.",
        )
    collector = await crud.collector.create(obj_in=collector_in)
    return collector


app.include_router(collectors_router,
                   prefix="/collectors", tags=["collectors"])



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


app.include_router(datasets_router,
                   prefix="/datasets", tags=["datasets"])
