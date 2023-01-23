import asyncio
from typing import List

from common import schemas
from fastapi import APIRouter, FastAPI, HTTPException
from uvicorn import Config, Server

from collector.collector_status import (
    Settings,
    CollectorBasicStatus,
    CollectorFullStatus,
    StatusManager,
)
from collector.config import settings

# Initialising the status object used by the API
collector_status = StatusManager()
collector_settings = Settings()

app = FastAPI()

# Settings
settings_router = APIRouter()


@settings_router.get("", response_model=List[schemas.collector.CollectorBase])
async def get_collectors():
    """
    Get the collectors configuration currently loaded.
    """
    return collector_settings.collectors


@settings_router.get("/{name}", response_model=schemas.collector.CollectorBase)
async def get_collector_with_name(*, name: str):
    """
    Get the settings for a given collector.
    """
    for collector in collector_settings.collectors:
        if collector.name == name:
            return collector
    raise HTTPException(status_code=404, detail="Collector not found")


app.include_router(settings_router, prefix="/settings", tags=["settings"])


# Status
status_router = APIRouter()


@status_router.get("/basic", response_model=List[CollectorBasicStatus])
async def get_status():
    return collector_status.collector_status


@status_router.get("/full", response_model=List[CollectorFullStatus])
async def get_full_status():
    return collector_status.collector_status


@status_router.get("/collector/{name}", response_model=CollectorFullStatus)
async def get_status_with_name(*, name: str):
    for collector in collector_status.collector_status:
        if collector.name == name:
            return collector
    raise HTTPException(status_code=404, detail="Collector not found")


app.include_router(status_router, prefix="/status", tags=["status"])


def start_api() -> asyncio.Task:
    collector_api = Server(
        Config(app, host=settings.collector_api_host, port=settings.collector_api_port)
    )
    return asyncio.create_task(collector_api.serve())
