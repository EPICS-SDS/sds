import asyncio
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from uvicorn import Config, Server

from esds.collector import collector_settings, collector_status
from esds.collector.collector_manager import (
    CollectorManager,
    CollectorNotFoundException,
)
from esds.collector.collector_status import CollectorBasicStatus, CollectorFullStatus
from esds.collector.config import settings
from esds.common import schemas
from esds.common.fast_api_offline import FastAPIOfflineDocs

description = """
This API can be used for:
- read and update the collector service configuration
- monitor the collectors' status and performance
- start/stop collectors
"""
app = FastAPIOfflineDocs(
    doc_cdon_files="static",
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
    flush_file_delay: float
    collector_timeout: float
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
        flush_file_delay=settings.flush_file_delay,
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


@settings_router.get("/collectors/{collector_id}", response_model=schemas.CollectorBase)
async def get_collector_with_collector_id(*, collector_id: str):
    """
    Get the settings for a given collector.
    Returns a 404 error if the collector is not loaded.
    """
    collector = collector_settings.collectors.get(collector_id, None)
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
    Returns a 409 error if a collector with the same `collector_id` is already loaded.
    """
    cm = CollectorManager.get_instance()
    if collector_in.collector_id in cm.collectors.keys():
        raise HTTPException(
            status_code=409,
            detail="The service already contains a collector with the requested collector_id.",
        )

    for collector in cm.collectors.values():
        if (
            collector_in.name == collector.name
            and collector_in.parent_path == collector.parent_path
            and collector_in.event_code == collector.event_code
            and sorted(collector_in.pvs) == sorted(collector.pvs)
        ):
            raise HTTPException(
                status_code=409,
                detail="The service already contains a collector with the requested configuration.",
            )

    collector = await cm.add_collector(collector_in)

    if start_collector:
        await cm.start_collector(collector.collector_id)

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
    response: Response,
):
    """
    Load several collectors to the service. Optionally, select if the collectors should be started or not after adding it.
    Returns a 206 status if a collector with the same collector_id is already loaded.
    """
    cm = CollectorManager.get_instance()
    errors = []
    collectors = []
    for collector_in in collectors_in:
        # Check each collector if they are already running
        if collector_in.collector_id in cm.collectors.keys():
            errors.append(collector_in.collector_id)

        for collector in cm.collectors.values():
            if (
                collector_in.name == collector.name
                and collector_in.parent_path == collector.parent_path
                and collector_in.event_code == collector.event_code
                and sorted(collector_in.pvs) == sorted(collector.pvs)
            ):
                errors.append(collector_in.collector_id)

        if errors != []:
            continue
        try:
            collector = await cm.add_collector(collector_in)
            collectors.append(collector)
        except Exception:
            # already logged exception
            errors.append(collector_in.name)
            continue
        if start_collector:
            await cm.start_collector(collector.collector_id)

    if settings.autosave_collectors_definition:
        await cm.save_configuration()

    if errors != []:
        response.status_code = status.HTTP_206_PARTIAL_CONTENT

    return collectors


@settings_router.delete(
    "/collector/{collector_id}",
    status_code=status.HTTP_200_OK,
)
async def remove_collector(*, collector_id: str, response: Response):
    """
    Remove a collector from the service.
    If the collector is running, it is first stopped.
    Returns a 404 message if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    if collector_id not in cm.collectors.keys():
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    await cm.remove_collector(collector_id)

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


@status_router.get("/collector/{collector_id}", response_model=CollectorFullStatus)
async def get_status_with_collector_id(*, collector_id: str):
    """
    Get the extended status for one collector.
    Returns a 404 error if the collector is not loaded.
    """
    collector = collector_status.collector_status_dict.get(collector_id, None)
    if collector is not None:
        return collector
    raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


@status_router.put("/collectors/start")
async def start_collectors(
    *,
    collector_ids: Optional[schemas.CollectorIdList] = schemas.CollectorIdList(
        collector_id=[]
    ),
    parent_path: Optional[str] = None,
    recursive: bool = True,
    timer: float = 0,
):
    """
    Start several or all the collectors loaded in the service.
    """
    cm = CollectorManager.get_instance()

    if collector_ids == [] and parent_path is None:
        await cm.start_all_collectors()
        return

    errors = []

    if collector_ids != []:
        collector_ids = collector_ids.collector_id

        for collector_id in collector_ids:
            try:
                await cm.start_collector(collector_id, timer)
            except CollectorNotFoundException:
                errors.append(collector_id)

    if parent_path is not None:
        for collector in cm.collectors.values():
            if (recursive and collector.parent_path.startswith(parent_path)) or (
                not recursive and collector.parent_path == parent_path
            ):
                try:
                    await cm.start_collector(collector.collector_id, timer)
                except CollectorNotFoundException:
                    errors.append(collector_id)

    if errors != []:
        raise HTTPException(
            status_code=404,
            detail=f"The following collectors were not found: {', '.join(errors)}",
        )


@status_router.put("/collectors/stop")
async def stop_collectors(
    *,
    collector_ids: Optional[schemas.CollectorIdList] = schemas.CollectorIdList(
        collector_id=[]
    ),
    parent_path: Optional[str] = None,
    recursive: bool = True,
):
    """
    Stop several or all the collectors loaded in the service.
    """
    cm = CollectorManager.get_instance()

    if collector_ids == [] and parent_path is None:
        await cm.stop_all_collectors()
        return

    errors = []

    if collector_ids.collector_id != []:
        collector_ids = collector_ids.collector_id

        for collector_id in collector_ids:
            try:
                await cm.stop_collector(collector_id)
            except CollectorNotFoundException:
                errors.append(collector_id)

    if parent_path is not None:
        for collector in cm.collectors.values():
            if (recursive and collector.parent_path.startswith(parent_path)) or (
                not recursive and collector.parent_path == parent_path
            ):
                try:
                    await cm.stop_collector(
                        collector.collector_id,
                    )
                except CollectorNotFoundException:
                    errors.append(collector_id)

    if errors != []:
        raise HTTPException(
            status_code=404,
            detail=f"The following collectors were not found: {', '.join(errors)}",
        )


@status_router.put("/collector/{collector_id}/start")
async def start_collector(*, collector_id: str, timer: float = 0):
    """
    Start a collector from the ones loaded in the service by specifying its `collector_id`.
    If the optional parameter `timer` is provided, the collector will stop after that time.
    Returns a 404 error if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    try:
        await cm.start_collector(collector_id, timer)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


@status_router.put("/collector/{collector_id}/stop")
async def stop_collector(*, collector_id: str):
    """
    Stop a collector from the ones loaded in the service by specifying its `collector_id`.
    If the collector is not running, it does nothing.
    Returns a 404 error if the collector is not loaded.
    """
    cm = CollectorManager.get_instance()
    try:
        await cm.stop_collector(collector_id)
    except CollectorNotFoundException:
        raise HTTPException(status_code=404, detail=COLLECTOR_NOT_FOUND)


app.include_router(status_router, prefix="/status", tags=["status"])


def start_api(root_path: str, reload: bool) -> asyncio.Task:
    collector_api = Server(
        Config(
            app,
            host=settings.collector_host,
            port=settings.collector_api_port,
            root_path=root_path,
            reload=reload,
        )
    )
    return asyncio.create_task(collector_api.serve())
