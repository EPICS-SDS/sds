import asyncio
from datetime import datetime
from typing import Optional, Set

from threading import Timer
import aiohttp
from pydantic import BaseModel

from app.config import settings
from app.dataset import Dataset
from app.event import Event


class CollectorSchema(BaseModel):
    name: str
    event_name: str
    event_code: int
    pvs: Set[str]


class Collector(CollectorSchema):
    id: str
    _dataset: Optional[Dataset] = None
    _timer: Optional[Timer] = None
    _task = None
    _background_tasks = set()

    class Config:
        underscore_attrs_are_private = True

    def update(self, event: Event):
        # If the PV does not belong to this event, ignore it.
        if event.pv_name not in self.pvs:
            return
        if self._task is None or self._task.done():
            queue = asyncio.Queue()
            dataset = Dataset(
                collector_id=self.id,
                name=self.name,
                trigger_date=datetime.utcnow(),
                trigger_pulse_id=event.trigger_pulse_id,
            )
            self._task = asyncio.create_task(self._collector(queue, dataset))
        queue.put_nowait(event)

    async def _collector(self, queue, dataset):
        async def consumer(queue, dataset):
            while True:
                event = await queue.get()
                print(f"Collector '{self.name}' received {repr(event)}")
                dataset.update(event)
                if self.is_dataset_complete(dataset):
                    return dataset
        coro = consumer(queue, dataset)
        timeout = settings.collector_timeout
        try:
            await asyncio.wait_for(coro, timeout=timeout)
            print(f"Collector '{self.name}' done")
        except asyncio.TimeoutError:
            print(f"Collector '{self.name}' timed out")

        # Run write and upload in a separate task so you don't end up with a
        # gap where wait_for is finished but the _collector task is not done.
        # Any events enqueued in that gap would never get processed.
        async def final(dataset):
            await dataset.upload()
            await dataset.write()
        task = asyncio.create_task(final(dataset))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def is_dataset_complete(self, dataset: Dataset):
        return all(map(lambda d: set(d) == self.pvs, dataset.entry))

    def event_matches(self, event: Event):
        if event.name != self.event_name:
            return False
        if event.pv_name not in self.pvs:
            return False
        return True
