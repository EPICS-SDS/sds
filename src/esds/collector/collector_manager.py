import asyncio
import logging
import os.path
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from typing import Dict, List, Optional
from urllib.parse import urljoin

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
                cl = CollectorList.model_validate(
                    [
                        CollectorDefinition.model_validate(coll)
                        for coll in self.collectors.values()
                    ]
                )
                await json_file.write(
                    cl.model_dump_json(
                        exclude={
                            "id",
                            "version",
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
        session_timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=settings.http_connection_timeout,
            sock_read=None,
        )
        async with aiohttp.ClientSession(
            json_serialize=CollectorBase.model_dump_json, timeout=session_timeout
        ) as session:
            logger.info(
                f"Adding collector '{os.path.join(collector_definition.parent_path, collector_definition.name)}'"
            )
            try:
                collector_definition_dict = collector_definition.model_dump()
                if collector_definition_dict["collector_id"] is None:
                    collector_definition_dict.pop("collector_id")

                new_collector = CollectorBase(**collector_definition_dict)

                async with session.post(
                    urljoin(str(settings.indexer_url), "/collectors"),
                    json=new_collector,
                ) as response:
                    response.raise_for_status()
                    if response.status == 201:
                        logger.info(
                            f"Collector '{os.path.join(new_collector.parent_path, new_collector.name)}' created in DB"
                        )
                    elif response.status == 200:
                        logger.info(
                            f"Collector '{os.path.join(new_collector.parent_path, new_collector.name)}' already in DB"
                        )
                    else:
                        logger.info(
                            f"Received response '{response.status}' while submitting collector to the indexer service."
                        )

                    obj = await response.json()
                    collector = Collector.model_validate(obj)
            except (
                ClientError,
                OSError,
            ) as e:
                logger.error(
                    f"Error submitting collector {os.path.join(new_collector.parent_path, new_collector.name)} to the indexer. Please check the indexer service status. Exception = {e}"
                )
                return None
            except Exception as e:
                logger.error(
                    f"Error submitting collector {os.path.join(new_collector.parent_path, new_collector.name)} to the indexer. Please check the indexer service status. Exception = {e}"
                )
                raise

        collector._pool = self._pool
        collector_status.add_collector(collector)
        self.collectors.update({collector.collector_id: collector})
        return collector

    async def remove_collector(self, collector_id: str):
        await self.stop_collector(collector_id)

        self.collectors.pop(collector_id)
        collector_status.remove_collector(collector_id)

    async def start_collector(self, collector_id: str, timer: float = 0):
        async with self.collector_lock:
            collector = self.collectors.get(collector_id)
            if collector is None:
                raise CollectorNotFoundException()

            # If collector is running, do nothing
            if collector_id in self.running_collectors:
                return

            # Clear any previous expired timer
            try:
                await self._timers[collector_id]
                self._timers.pop(collector_id)
            except KeyError:
                pass

            new_pvs = {pv for pv in collector.pvs if pv not in self._tasks.keys()}

            # Subscribe to each new PV and store the task reference
            self._tasks.update(
                {pv: asyncio.create_task(self._subscribe(pv)) for pv in new_pvs}
            )

            self.running_collectors.append(collector_id)
            collector_status.set_collector_running(collector_id, True)

            if timer > 0:
                timer_task = asyncio.create_task(
                    self._stop_collector_after_timer(
                        collector_id=collector_id, timer=timer
                    )
                )

                self._timers.update({collector_id: timer_task})

    async def _stop_collector_after_timer(self, collector_id: str, timer: float):
        await asyncio.sleep(timer)
        async with self.collector_lock:
            await self._stop_collector(collector_id)

    async def stop_collector(self, collector_id: str):
        async with self.collector_lock:
            # Clear any running timer
            try:
                self._timers[collector_id].cancel()
                try:
                    await self._timers[collector_id]
                except asyncio.CancelledError:
                    pass
                self._timers.pop(collector_id)
            except KeyError:
                pass

            await self._stop_collector(collector_id)

    async def _stop_collector(self, collector_id: str):
        collector = self.collectors.get(collector_id)
        if collector is None:
            raise CollectorNotFoundException()

        try:
            self.running_collectors.remove(collector_id)
        except ValueError:
            # If collector is not running, do nothing
            return

        pvs_to_keep = {
            pv
            for (collector_id, collector) in self.collectors.items()
            if collector_id in self.running_collectors
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

        collector_status.set_collector_running(collector_id, False)

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
            for collector_id in self.collectors.keys():
                self.running_collectors.append(collector_id)
                collector_status.set_collector_running(collector_id, True)

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

            for collector_id in self.collectors.keys():
                collector_status.set_collector_running(collector_id, False)

            self.running_collectors.clear()

    async def wait_for_startup(self, timeout=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            all_pv_connected = True
            for collector in collector_status.pv_status_dict.values():
                for pv in collector.values():
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
                collector_status.set_pv_connected(pv, False)
                logger.info(f"PV '{pv}' disconnected, reconnecting in {self._timeout}s")
            except asyncio.CancelledError:
                collector_status.set_pv_connected(pv, False)
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
            if (
                collector.collector_id in self.running_collectors
                and collector.event_matches(event)
            ):
                collector.update(event)

    async def join(self):
        await self.shutdown_event.wait()
