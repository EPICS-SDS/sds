import asyncio
from contextlib import asynccontextmanager
from typing import Set

from common.files import Event
from p4p.client.asyncio import Context, Disconnected
from pydantic import ValidationError

from collector.collector import Collector
from collector.config import settings
from collector.epics_event import EpicsEvent


class AsyncSubscription:
    def __init__(self, context: Context, pv):
        self._context = context
        self._pv = pv
        self._on_message = None

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        async def cb(value):
            if self._on_message:
                self._on_message(value)

        self._sub = self._context.monitor(self._pv, cb)

    def close(self):
        self._sub.close()

    @asynccontextmanager
    async def messages(self):
        cb, generator = self._cb_and_generator()
        self._on_message = cb
        yield generator

    def _cb_and_generator(self):
        queue = asyncio.Queue()

        def _put_in_queue(message):
            queue.put_nowait(message)

        async def _message_generator():
            while True:
                yield await queue.get()

        return _put_in_queue, _message_generator()


class CollectorManager:
    def __init__(
        self,
        collectors: Set[Collector],
        timeout: int = settings.collector_timeout,
    ):
        self._context = Context("pva")
        self._timeout = timeout
        self.collectors = set(collectors)
        self.startup_event = asyncio.Event()
        self.startup_lock = asyncio.Lock()

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
                            if isinstance(message, Exception):
                                raise message
                            self._message_handler(pv, message)
                print(f"PV '{pv}' subscription ended")
            except Disconnected:
                print(f"PV '{pv}' disconnected, reconnecting in {self._timeout}s")
            except Exception as e:
                print(f"PV '{pv}' error, closing subscription")
                print(e)
                return
            finally:
                await asyncio.sleep(self._timeout)

    def _message_handler(self, pv, message):
        try:
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
