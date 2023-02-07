import asyncio
from contextlib import asynccontextmanager

from collector.api import collector_status
from collector.collector_status import PvStatus
from p4p.client.asyncio import Context, Disconnected


class AsyncSubscription:
    """
    A subscription (monitor) to a PV that queues all received events and
    provides a generator that yields the events from the queue.
    """

    def __init__(self, context: Context, pv):
        self._context = context
        self._pv = pv
        self._on_message = None
        self.pv_status: PvStatus
        self.queue = asyncio.Queue()

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        """
        Starts a PV monitor that will process messages only when a generator returned by the :func:`messages` method
        """
        self.pv_status = collector_status.get_pv_status(self._pv)
        self.cb = self._first_cb
        self._sub = self._context.monitor(
            self._pv, self.callback, notify_disconnect=True
        )

    def close(self):
        """Stops the PV monitor."""
        self._sub.close()

    async def callback(self, value):
        self.cb(value)

    def _first_cb(self, value):
        """Ignore the first update that ocurred in the past"""
        if isinstance(value, Disconnected):
            print(f"PV '{self._pv}' disconnected")
            self.pv_status.connected = False
        elif isinstance(value, Exception):
            raise value
        else:
            self.cb = self._cb
            self.pv_status.connected = True

    def _cb(self, value):
        if isinstance(value, Disconnected):
            print(f"PV '{self._pv}' disconnected")
            self.pv_status.connected = False
            self.cb = self._first_cb
        elif isinstance(value, Exception):
            raise value
        else:
            collector_status.set_update_event(self._pv)
            self.queue.put_nowait(value)

    @asynccontextmanager
    async def messages(self):
        """Yields a generator that can be iterated to receive all updates from the PV."""
        yield self._message_generator()

    async def _message_generator(self):
        while True:
            yield await self.queue.get()
