import asyncio
from typing import List

from collector import collector_settings, collector_status
from collector.collector_manager import CollectorManager, CollectorNotFoundException
from collector.collector_status import CollectorBasicStatus, CollectorFullStatus
from collector.config import settings
from common import schemas
from fastapi import APIRouter, FastAPI, HTTPException, Response, status
from uvicorn import Config, Server

description = """
This API can be used for:
- read and update the collector service configuration
- monitor the collectors' status and performance
- start/stop collectors
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
    return list(collector_settings.collectors.values())


@settings_router.get("/{name}", response_model=schemas.collector.CollectorBase)
async def get_collector_with_name(*, name: str):
    """
    Get the settings for a given collector.
    Returns a 404 error if the collector is not loaded.
    """
    collector = collector_settings.collectors.get(name, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail="Collector not found")


@settings_router.post(
    "/collector",
    response_model=schemas.CollectorBase,
    status_code=status.HTTP_201_CREATED,
)
async def add_collector(
    *, start_collector: bool = True, collector_in: schemas.CollectorBase
):
    """
    Load a collector to the service. Optionally, select if the collector should be started or not after adding it.
    Returns a 409 error if a collector with the same name is already loaded.
    """
    cm = CollectorManager.get_instance()
    if collector_in.name in cm.collectors.keys():
        raise HTTPException(
            status_code=409,
            detail="The service already contains a collector with the requested name.",
        )
    collector = await cm.add_collector(collector_in)
    if start_collector:
        await cm.start_collector(collector_in.name)
    return collector


@settings_router.delete(
    "/collector/{name}",
    status_code=status.HTTP_200_OK,
)
async def remove_collector(*, name: str, response: Response):
    """
    Remove a collector from the service.
    If the collector is running, it is first stopped.
    Returns a 204 message if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    if name not in cm.collectors.keys():
        response.status_code = status.HTTP_204_NO_CONTENT
        return
    await cm.remove_collector(name)


app.include_router(settings_router, prefix="/settings", tags=["settings"])


# Status
status_router = APIRouter()


@status_router.get("/basic", response_model=List[CollectorBasicStatus])
async def get_status():
    """
    Get status from all collectors. This view only shows if collectors are running or not,
    when was the last event received, and the time it takes to collect a dataset.
    """
    return list(collector_status.collector_status_dict.values())


@status_router.get("/full", response_model=List[CollectorFullStatus])
async def get_full_status():
    """
    Get extended status from all collectors. This view shows information about all PVs monitored by each collector.
    """
    return list(collector_status.collector_status_dict.values())


@status_router.get("/collector/{name}", response_model=CollectorFullStatus)
async def get_status_with_name(*, name: str):
    """
    Get the extended status for one collector.
    Returns a 404 error if the collector is not loaded.
    """
    collector = collector_status.collector_status_dict.get(name, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail="Collector not found")


@status_router.put("/collectors/start")
async def start_all_collectors():
    """
    Start all the collectors loaded in the service.
    """
    cm = CollectorManager.get_instance()
    await cm.start_all_collectors()


@status_router.put("/collectors/stop")
async def stop_all_collectors():
    """
    Stop all the collectors loaded in the service.
    """
    cm = CollectorManager.get_instance()
    await cm.stop_all_collectors()


@status_router.put("/collector/{name}/start")
async def start_collector(*, name: str):
    """
    Start a collector from the ones loaded in the service by specifying its name.
    Returns a 404 error if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    try:
        await cm.start_collector(name)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail="Collector not found")


@status_router.put("/collector/{name}/stop")
async def stop_collector(*, name: str):
    """
    Stop a collector from the ones loaded in the service by specifying its name.
    If the collector is not running, it does nothing.
    Returns a 404 error if the collector is not loaded.
    """
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
