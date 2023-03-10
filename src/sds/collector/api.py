import asyncio
from typing import List

from fastapi import APIRouter, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sds.collector import collector_settings, collector_status
from sds.collector.collector_manager import CollectorManager, CollectorNotFoundException
from sds.collector.collector_status import CollectorBasicStatus, CollectorFullStatus
from sds.collector.config import settings
from sds.common import schemas
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

COLLECTOR_NOT_FOUND = "Collector not found"

# Settings
settings_router = APIRouter()


class CollectorSettingsSchema(BaseModel):
    epics_addr_list: str
    collector_timeout: int
    events_per_file: int
    autostart_collectors: bool
    status_queue_length: int


@settings_router.get("", response_model=CollectorSettingsSchema)
async def get_settings():
    """
    Get the settings of this collector services instance.
    """

    cm = CollectorManager.get_instance()
    epics_settings = cm._context.conf()

    return CollectorSettingsSchema(
        epics_addr_list=epics_settings["EPICS_PVA_ADDR_LIST"],
        collector_timeout=settings.collector_timeout,
        events_per_file=settings.events_per_file,
        autostart_collectors=settings.autostart_collectors,
        status_queue_length=settings.status_queue_length,
    )


@settings_router.get("/collectors", response_model=List[schemas.CollectorBase])
async def get_collectors():
    """
    Get the collectors configuration currently loaded.
    """
    return list(collector_settings.collectors.values())


@settings_router.get("/collectors/{name}", response_model=schemas.CollectorBase)
async def get_collector_with_name(*, name: str):
    """
    Get the settings for a given collector.
    Returns a 404 error if the collector is not loaded.
    """
    collector = collector_settings.collectors.get(name, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


@settings_router.put(
    "/collectors/save",
    status_code=status.HTTP_200_OK,
)
async def save_collectors_definition():
    """
    Overwrite the collectors definition file with the current configuration.
    If autosave is enabled, this method does nothing.
    """
    if settings.autosave_collectors_definition:
        return "Autosave is enabled. Collectors definition file already up-to-date."
    else:
        cm = CollectorManager.get_instance()
        await cm.save_configuration()


@settings_router.post(
    "/collector",
    response_model=schemas.CollectorBase,
    status_code=status.HTTP_201_CREATED,
)
async def add_collector(
    *, start_collector: bool = True, collector_in: schemas.CollectorDefinition
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

    if settings.autosave_collectors_definition:
        await cm.save_configuration()

    return collector


@settings_router.post(
    "/collectors",
    response_model=List[schemas.CollectorBase],
    status_code=status.HTTP_201_CREATED,
)
async def add_collectors(
    *,
    start_collector: bool = True,
    collectors_in: List[schemas.CollectorDefinition],
    response: Response
):
    """
    Load several collectors to the service. Optionally, select if the collectors should be started or not after adding it.
    Returns a 206 status if a collector with the same name is already loaded.
    """
    cm = CollectorManager.get_instance()
    errors = []
    collectors = []
    for collector_in in collectors_in:
        if collector_in.name in cm.collectors.keys():
            errors.append(collector_in.name)
            continue
        collector = await cm.add_collector(collector_in)
        collectors.append(collector)
        if start_collector:
            await cm.start_collector(collector_in.name)

    if settings.autosave_collectors_definition:
        await cm.save_configuration()

    if errors != []:
        response.status_code = status.HTTP_206_PARTIAL_CONTENT

    return collectors


@settings_router.delete(
    "/collector/{name}",
    status_code=status.HTTP_200_OK,
)
async def remove_collector(*, name: str, response: Response):
    """
    Remove a collector from the service.
    If the collector is running, it is first stopped.
    Returns a 404 message if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    if name not in cm.collectors.keys():
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    await cm.remove_collector(name)

    if settings.autosave_collectors_definition:
        await cm.save_configuration()


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
    raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


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
async def start_collector(*, name: str, timer: float = 0):
    """
    Start a collector from the ones loaded in the service by specifying its name.
    If the optional parameter `timer` is provided, the collector will stop after that time.
    Returns a 404 error if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    try:
        await cm.start_collector(name, timer)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


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
        raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


app.include_router(status_router, prefix="/status", tags=["status"])


def start_api() -> asyncio.Task:
    collector_api = Server(
        Config(app, host=settings.collector_host, port=settings.collector_api_port)
    )
    return asyncio.create_task(collector_api.serve())
