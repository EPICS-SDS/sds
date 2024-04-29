import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from typing import Dict, List, Optional

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientError
from p4p.client.asyncio import Context, Disconnected
from pydantic import ValidationError

from esds.collector import collector_settings, collector_status
from esds.collector.async_subscription import AsyncSubscription
from esds.collector.collector import Collector
from esds.collector.config import settings
from esds.collector.epics_event import EpicsEvent
from esds.common.files import CollectorDefinition, CollectorList, Event
from esds.common.schemas import CollectorBase

logger = logging.getLogger(__name__)


class CollectorNotFoundException(Exception):
    pass


class CollectorManager:
    """
    This class handles the subscriptions to PVs (monitors).
    Different collectors may be connecting to the same PV and this way we only start one subscription per PV.
    """

    instance = None

    def __init__(
        self,
        timeout: int,
    ):
        self._context = Context("pva", nt=False)
        self._timeout = timeout
        self._tasks: Dict[str, asyncio.Task] = dict()
        self.collectors: Dict[str, Collector] = dict()
        self._timers: Dict[str, asyncio.Task] = dict()
        self.shutdown_event = asyncio.Event()
        self.collector_lock = asyncio.Lock()
        self.running_collectors: List[str] = []
        self._pool: ProcessPoolExecutor = ProcessPoolExecutor(
            max_workers=max(1, cpu_count() - 1)
        )

        collector_settings.collectors = self.collectors
        collector_status.collector_status_dict.clear()
        collector_status.pv_status_dict.clear()

    @classmethod
    async def create(
        cls,
        collectors: Optional[List[CollectorBase]],
        timeout: int = settings.collector_timeout,
    ):
        cls.instance = CollectorManager(timeout)

        adding_collector_tasks = []
        if collectors is not None:
            for collector in collectors:
                adding_collector_tasks.append(
                    asyncio.create_task(cls.instance.add_collector(collector))
                )

            await asyncio.gather(*adding_collector_tasks)

        return cls.instance

    async def __aenter__(self):
        if settings.autostart_collectors:
            await self.start_all_collectors()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @classmethod
    def get_instance(cls):
        return cls.instance

    async def save_configuration(self):
        path = settings.collector_definitions

        try:
            async with aiofiles.open(path, mode="w") as json_file:
                cl = CollectorList.parse_obj(
                    [
                        CollectorDefinition.parse_obj(coll)
                        for coll in self.collectors.values()
                    ]
                )
                await json_file.write(
                    cl.json(
                        exclude={
                            "id",
                        },
                        indent=4,
                    )
                )
        except PermissionError:
            logger.warning(
                "Collector definition file not writable. Configuration changes could not be saved."
            )

    async def add_collector(self, collector_definition: CollectorDefinition):
        # First register the collector in the indexer service
        session_timeout = aiohttp.ClientTimeout(total=None,sock_connect=settings.http_connection_timeout,sock_read=settings.http_connection_timeout)
        async with aiohttp.ClientSession(json_serialize=CollectorBase.model_dump_json, timeout=session_timeout) as session:
            logger.info(f"Adding collector '{collector_definition.name}'")
            try:
                new_collector = CollectorBase(
                    **collector_definition.dict(), host=settings.collector_host
                )
                async with session.post(
                    settings.indexer_url + "/collectors",
                    json=new_collector,
                ) as response:
                    response.raise_for_status()
                    if response.status == 201:
                        logger.info(f"Collector '{new_collector.name}' created in DB")
                    elif response.status == 200:
                        logger.info(f"Collector '{new_collector.name}' already in DB")

                    obj = await response.json()
                    collector = Collector.parse_obj(obj)
            except (
                ClientError,
                OSError,
            ):
                logger.error(
                    f"Error submitting collector {new_collector.name} to the indexer. Please check the indexer service status."
                )
                raise

        collector._pool = self._pool
        collector_status.add_collector(collector)
        self.collectors.update({collector.name: collector})
        return collector

    async def remove_collector(self, collector_name: str):
        await self.stop_collector(collector_name)

        self.collectors.pop(collector_name)
        collector_status.remove_collector(collector_name)

    async def start_collector(self, name: str, timer: float = 0):
        async with self.collector_lock:
            collector = self.collectors.get(name)
            if collector is None:
                raise CollectorNotFoundException()

            # If collector is running, do nothing
            if name in self.running_collectors:
                return

            # Clear any previous expired timer
            try:
                await self._timers[name]
                self._timers.pop(name)
            except KeyError:
                pass

            new_pvs = {pv for pv in collector.pvs if pv not in self._tasks.keys()}

            # Subscribe to each new PV and store the task reference
            self._tasks.update(
                {pv: asyncio.create_task(self._subscribe(pv)) for pv in new_pvs}
            )

            self.running_collectors.append(name)
            collector_status.set_collector_running(name, True)

            if timer > 0:
                timer_task = asyncio.create_task(
                    self._stop_collector_after_timer(name=name, timer=timer)
                )

                self._timers.update({name: timer_task})

    async def _stop_collector_after_timer(self, name: str, timer: float):
        await asyncio.sleep(timer)
        async with self.collector_lock:
            await self._stop_collector(name)

    async def stop_collector(self, name: str):
        async with self.collector_lock:
            # Clear any running timer
            try:
                self._timers[name].cancel()
                try:
                    await self._timers[name]
                except asyncio.CancelledError:
                    pass
                self._timers.pop(name)
            except KeyError:
                pass

            await self._stop_collector(name)

    async def _stop_collector(self, name: str):
        collector = self.collectors.get(name)
        if collector is None:
            raise CollectorNotFoundException()

        try:
            self.running_collectors.remove(name)
        except ValueError:
            # If collector is not running, do nothing
            return

        pvs_to_keep = {
            pv
            for (collector_name, collector) in self.collectors.items()
            if collector_name in self.running_collectors
            for pv in collector.pvs
        }

        pvs_to_remove = [pv for pv in collector.pvs if pv not in pvs_to_keep]

        # Subscribe to each new PV and store the task reference
        canceled_tasks = []
        for pv in pvs_to_remove:
            task = self._tasks.pop(pv)
            task.cancel()
            canceled_tasks.append(task)

        if canceled_tasks != []:
            await asyncio.wait(canceled_tasks)

        collector_status.set_collector_running(name, False)

    async def start_all_collectors(self):
        """Start all the collectors in the ColectorManager"""
        async with self.collector_lock:
            # Clear any existing timer (those collectors will continue to run)
            if len(self._timers) > 0:
                for timer_task in self._timers.values():
                    timer_task.cancel()
                await asyncio.wait(self._timers.values())
                self._timers.clear()

            # Collect PVs into a set to remove duplicates
            new_pvs = {
                pv
                for collector in self.collectors.values()
                for pv in collector.pvs
                if pv not in self._tasks.keys()
            }

            # Subscribe to each new PV and store the task reference
            self._tasks.update(
                {pv: asyncio.create_task(self._subscribe(pv)) for pv in new_pvs}
            )

            # Update collector status
            self.running_collectors.clear()
            for collector_name in self.collectors.keys():
                self.running_collectors.append(collector_name)
                collector_status.set_collector_running(collector_name, True)

    async def stop_all_collectors(self):
        """Stop all the collectors in the ColectorManager"""
        async with self.collector_lock:
            # Clear any existing timer
            if len(self._timers) > 0:
                for timer_task in self._timers.values():
                    timer_task.cancel()
                await asyncio.wait(self._timers.values())
                self._timers.clear()

            if len(self._tasks) > 0:
                for task in self._tasks.values():
                    task.cancel()
                await asyncio.wait(self._tasks.values())
                self._tasks.clear()

            for collector_name in self.collectors.keys():
                collector_status.set_collector_running(collector_name, False)

            self.running_collectors.clear()

    async def wait_for_startup(self, timeout=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            all_pv_connected = True
            for pv in collector_status.pv_status_dict.values():
                all_pv_connected *= pv.pv_status.connected
            if all_pv_connected:
                logger.info(
                    f"Collector start-up completed in {time.time() - start_time} s."
                )
                return
            await asyncio.sleep(0.1)

    async def close(self):
        await self.stop_all_collectors()
        self._context.close()
        self._pool.shutdown()
        self.shutdown_event.set()

    async def _subscribe(self, pv):
        while True:
            try:
                logger.info(f"PV '{pv}' subscribing...")
                async with AsyncSubscription(self._context, pv) as sub:
                    async with sub.messages() as messages:
                        logger.info(f"PV '{pv}' subscribed!")
                        async for message in messages:
                            self._message_handler(pv, message)
                logger.info(f"PV '{pv}' subscription ended")
            except Disconnected:
                collector_status.get_pv_status(pv).connected = False
                logger.info(f"PV '{pv}' disconnected, reconnecting in {self._timeout}s")
            except asyncio.CancelledError:
                collector_status.get_pv_status(pv).connected = False
                logger.info(f"PV '{pv}' subscription closed")
                return

    def _message_handler(self, pv, message):
        try:
            event = EpicsEvent(pv_name=pv, value=message)
        except ValidationError as e:
            logger.error(f"PV '{pv}' event validation error")
            logger.error(e)
            return

        self._event_handler(event)

    # Finds the event and updates it with the value
    def _event_handler(self, event: Event):
        for collector in self.collectors.values():
            if collector.name in self.running_collectors and collector.event_matches(
                event
            ):
                collector.update(event)

    async def join(self):
        await self.shutdown_event.wait()
