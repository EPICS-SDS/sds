import asyncio
from datetime import datetime
from typing import Dict, FrozenSet, Set

from common.files import Event
from collector.indexable_dataset import IndexableDataset
from pydantic import BaseModel

from collector.config import settings


class CollectorSchema(BaseModel):
    name: str
    event_name: str
    event_code: int
    pvs: FrozenSet[str]


class Collector(CollectorSchema):
    id: str
    _tasks: Set[asyncio.Task] = set()
    _queues: Dict[int, asyncio.Queue] = dict()
    _timeout: int = settings.collector_timeout

    class Config:
        frozen = True
        underscore_attrs_are_private = True

    def has_queue(self, id: int):
        return id in self._queues

    def get_queue(self, id: int):
        if self.has_queue(id):
            return self._queues[id]
        else:
            queue = asyncio.Queue()
            self._queues[id] = queue
            return queue

    def discard_queue(self, id: int):
        del self._queues[id]

    def update(self, event: Event):
        # If the PV does not belong to this event, ignore it.
        if not self.event_matches(event):
            print(repr(self), f"received bad event {repr(event)}")
            return

        if not self.has_queue(event.trigger_pulse_id):
            queue = self.get_queue(event.trigger_pulse_id)
            dataset = IndexableDataset(
                collector_id=self.id,
                collector_name=self.name,
                trigger_date=datetime.utcnow(),
                trigger_pulse_id=event.trigger_pulse_id,
                event_name=event.timing_event_name,
                event_code=event.timing_event_code,
                data_date=[event.data_date],
                data_pulse_id=[event.pulse_id],
            )
            task = asyncio.create_task(self._collector(queue, dataset))
            self._tasks.add(task)

            def task_done_cb(task):
                self._tasks.discard(task)
                self.discard_queue(event.trigger_pulse_id)

            task.add_done_callback(task_done_cb)

        queue = self.get_queue(event.trigger_pulse_id)
        queue.put_nowait(event)

    async def _collector(self, queue, dataset):
        async def consumer(queue, dataset):
            while True:
                event = await queue.get()
                dataset.update(event)

        coro = consumer(queue, dataset)
        try:
            await asyncio.wait_for(coro, self._timeout)
        except asyncio.TimeoutError:
            pass

        await dataset.index()
        await dataset.write()

    def event_matches(self, event: Event):
        if event.timing_event_code != self.event_code:
            return False
        if event.pv_name not in self.pvs:
            return False
        return True
