import asyncio
from contextlib import asynccontextmanager

from p4p.client.asyncio import Context


class AsyncSubscription:
    """
    A subscription (monitor) to a PV that queues all received events and
    provides a generator that yields the events from the queue.
    """

    def __init__(self, context: Context, pv):
        self._context = context
        self._pv = pv
        self._on_message = None
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
        self._sub = self._context.monitor(self._pv, self._cb)

    def close(self):
        """Stops the PV monitor."""
        self._sub.close()

    async def _cb(self, value):
        self.queue.put_nowait(value)

    @asynccontextmanager
    async def messages(self):
        """Yields a generator that can be iterated to receive all updates from the PV."""
        yield self._message_generator()

    async def _message_generator(self):
        while True:
            yield await self.queue.get()
