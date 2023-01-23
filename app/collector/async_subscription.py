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

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        """
        Starts a PV monitor that will process messages only when a generator returned by the :func:`messages` method
        """

        async def cb(value):
            if self._on_message:
                self._on_message(value)

        self._sub = self._context.monitor(self._pv, cb)

    def close(self):
        """Stops the PV monitor."""
        self._sub.close()

    @asynccontextmanager
    async def messages(self):
        """Yields a generator that can be iterated to receive all updates from the PV."""
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
