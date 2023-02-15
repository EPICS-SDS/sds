import asyncio
from typing import List

from collector.collector_manager import CollectorManager, CollectorNotFoundException
from collector.collector_status import (
    CollectorBasicStatus,
    CollectorFullStatus,
)
from collector import (
    collector_settings,
    collector_status,
)
from collector.config import settings
from common import schemas
from fastapi import APIRouter, FastAPI, HTTPException
from uvicorn import Config, Server


description = """
This API can be used for:
- read and update the collector service configuration
- monitor the collectors' status and performance
"""
app = FastAPI(
    title="SDS Collector Service API",
    description=description,
    version="0.1",
)

# Settings
settings_router = APIRouter()


@settings_router.get("", response_model=List[schemas.collector.CollectorBase])
async def get_collectors():
    """
    Get the collectors configuration currently loaded.
    """
    return set(collector_settings.collectors.values())


@settings_router.get("/{name}", response_model=schemas.collector.CollectorBase)
async def get_collector_with_name(*, name: str):
    """
    Get the settings for a given collector.
    """
    collector = collector_settings.collectors.get(name, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail="Collector not found")


app.include_router(settings_router, prefix="/settings", tags=["settings"])


# Status
status_router = APIRouter()


@status_router.get("/basic", response_model=List[CollectorBasicStatus])
async def get_status():
    return list(collector_status.collector_status_dict.values())


@status_router.get("/full", response_model=List[CollectorFullStatus])
async def get_full_status():
    return list(collector_status.collector_status_dict.values())


@status_router.get("/collector/{name}", response_model=CollectorFullStatus)
async def get_status_with_name(*, name: str):
    collector = collector_status.collector_status_dict.get(name, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail="Collector not found")


@status_router.put("/collectors/start")
async def start_all_collector():
    cm = CollectorManager.get_instance()
    await cm.start_all_collectors()


@status_router.put("/collectors/stop")
async def stop_all_collector():
    cm = CollectorManager.get_instance()
    await cm.stop_all_collectors()


@status_router.put("/collector/{name}/start")
async def start_collector(*, name: str):
    cm = CollectorManager.get_instance()
    try:
        await cm.start_collector(name)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail="Collector not found")


@status_router.put("/collector/{name}/stop")
async def stop_collector(*, name: str):
    cm = CollectorManager.get_instance()
    try:
        await cm.stop_collector(name)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail="Collector not found")


app.include_router(status_router, prefix="/status", tags=["status"])


def start_api() -> asyncio.Task:
    collector_api = Server(
        Config(app, host=settings.collector_api_host, port=settings.collector_api_port)
    )
    return asyncio.create_task(collector_api.serve())
