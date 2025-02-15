import asyncio
import logging
from contextlib import asynccontextmanager

from p4p.client.asyncio import Context, Disconnected

from esds.collector import collector_status
from esds.collector.collector_status import PvStatus

logger = logging.getLogger(__name__)


class AsyncSubscription:
    """
    A subscription (monitor) to a PV that queues all received events and
    provides a generator that yields the events from the queue.
    """

    def __init__(self, context: Context, pv: str):
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
            collector_status.set_pv_connected(self._pv, connected=False)
        elif isinstance(value, Exception):
            logger.warning(f"PV '{self._pv}' event raised an exception {value}.")
            raise value
        else:
            logger.info(f"PV '{self._pv}' connected")
            self.cb = self._cb
            collector_status.set_pv_connected(self._pv, connected=True)

    def _cb(self, value):
        if isinstance(value, Disconnected):
            logger.info(f"PV '{self._pv}' disconnected")
            collector_status.set_pv_connected(self._pv, connected=False)
            self.cb = self._first_cb
        elif isinstance(value, Exception):
            logger.warning(f"PV '{self._pv}' event raised an exception {value}.")
            raise value
        else:
            if "value" not in value.keys():
                logger.warning(f"PV '{self._pv}' has no value field, discarding event.")
                return

            self.queue.put_nowait(value)

    @asynccontextmanager
    async def messages(self):
        """Yields a generator that can be iterated to receive all updates from the PV."""
        yield self._message_generator()

    async def _message_generator(self):
        while True:
            yield await self.queue.get()
