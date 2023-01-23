import asyncio
from datetime import datetime
from typing import Set

from collector.api import collector_settings, collector_status
from collector.async_subscription import AsyncSubscription
from collector.collector import Collector
from collector.config import settings
from collector.epics_event import EpicsEvent
from common.files import Event
from p4p.client.asyncio import Context, Disconnected
from pydantic import ValidationError


class CollectorManager:
    """
    This class handles the subscriptions to PVs (monitors).
    Different collectors may be connecting to the same PV and this way we only start one subscription per PV.
    """

    def __init__(
        self,
        collectors: Set[Collector],
        timeout: int = settings.collector_timeout,
    ):
        self._context = Context("pva")
        self._timeout = timeout
        self.collectors = collectors
        self.startup_event = asyncio.Event()
        self.startup_lock = asyncio.Lock()

        collector_settings.collectors = self.collectors

        for collector in self.collectors:
            collector_status.add_collector(collector)

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    # Start monitoring each PV
    def start(self):
        # Collect PVs into a set to remove duplicates
        pvs = {pv for collector in self.collectors for pv in collector.pvs}
        # Subscribe to each PV and store the task reference
        self.n_subscriptions = len(pvs)
        self._tasks = [asyncio.create_task(self._subscribe(pv)) for pv in pvs]

    async def wait_for_startup(self):
        await self.startup_event.wait()

    def close(self):
        self._context.close()
        for task in self._tasks:
            task.cancel()

    async def _subscribe(self, pv):
        while True:
            try:
                print(f"PV '{pv}' subscribing...")
                async with AsyncSubscription(self._context, pv) as sub:
                    # Trigger the event when startup is finished
                    async with self.startup_lock:
                        if self.n_subscriptions > 0:
                            self.n_subscriptions -= 1
                        if self.n_subscriptions == 0:
                            self.startup_event.set()
                    async with sub.messages() as messages:
                        print(f"PV '{pv}' subscribed!")
                        async for message in messages:
                            collector_status.set_connected(pv, True)
                            if isinstance(message, Disconnected):
                                print(f"PV '{pv}' disconnected")
                                collector_status.set_connected(pv, False)
                            elif isinstance(message, Exception):
                                raise message
                            else:
                                self._message_handler(pv, message)
                print(f"PV '{pv}' subscription ended")
            except Disconnected:
                print(f"PV '{pv}' disconnected, reconnecting in {self._timeout}s")
            except Exception as e:
                print(f"PV '{pv}' error, closing subscription")
                print(e)
                return

    def _message_handler(self, pv, message):
        try:
            collector_status.pvs[pv].last_event = datetime.utcnow()
            event = EpicsEvent(pv_name=pv, value=message)
        except ValidationError:
            print(f"PV '{pv}' event validation error")
            return
        self._event_handler(event)

    # Finds the event and updates it with the value
    def _event_handler(self, event: Event):
        for collector in self.collectors:
            if collector.event_matches(event):
                collector.update(event)

    async def join(self):
        await asyncio.wait(self._tasks)
