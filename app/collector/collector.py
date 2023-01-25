from asyncio import Queue, Task, TimeoutError, create_task, wait_for
from datetime import datetime
from threading import Lock
from typing import Dict, List, Set

from collector.api import collector_status
from collector.config import settings
from common.files import Event, NexusFile
from common.schemas import CollectorBase


class Collector(CollectorBase):
    id: str
    _tasks: Set[Task] = set()
    _queues: Dict[int, Queue] = dict()
    _timeout: int = settings.collector_timeout
    _events_per_file: int = settings.events_per_file
    _file_lock: Lock
    _files: Dict[int, NexusFile] = dict()
    _concurrent_events: Dict[str, List[Queue]] = dict()

    class Config:
        frozen = True
        underscore_attrs_are_private = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._file_lock = Lock()

    def get_file(self, id: int) -> NexusFile:
        for f in self._files.keys():
            if id >= f and id < f + self._events_per_file:
                return self._files[f]
        return None

    def discard_file(self, f: NexusFile):
        for key, value in dict(self._files).items():
            if value == f:
                del self._files[key]

    def get_queue(self, id: int):
        return self._queues.get(id)

    def create_queue(self, id: int):
        queue = Queue()
        self._queues[id] = queue
        return queue

    def discard_queue(self, id: int):
        del self._queues[id]

    def update(self, event: Event):
        # If the PV does not belong to this event, ignore it.
        if not self.event_matches(event):
            print(repr(self), f"received bad event {repr(event)}")
            return

        with self._file_lock:
            nexus_file = self.get_file(event.trigger_pulse_id)

            if nexus_file is None:
                # First create a new file
                nexus_file = NexusFile(
                    collector_id=self.id,
                    collector_name=self.name,
                    trigger_date=event.trigger_date,
                    trigger_pulse_id=event.trigger_pulse_id,
                    event_code=event.timing_event_code,
                )
                self._files[event.trigger_pulse_id] = nexus_file
                self._concurrent_events[nexus_file.name] = []

        # One queue per trigger_id
        with self._file_lock:
            queue = self._queues.get(event.trigger_pulse_id)
            if queue is None:
                queue = self.create_queue(event.trigger_pulse_id)

                self._concurrent_events[nexus_file.name].append(queue)

                task = create_task(self._collector(queue, nexus_file))
                self._tasks.add(task)

                def task_done_cb(task):
                    self._tasks.discard(task)
                    self.discard_queue(event.trigger_pulse_id)

                task.add_done_callback(task_done_cb)

        queue.put_nowait(event)

    async def _collector(self, queue: Queue, nexus_file: NexusFile):
        first_update_received = datetime.utcnow()
        last_update_received = [datetime.utcnow()]

        async def consumer(queue: Queue, nexus_file: NexusFile):
            while True:
                event = await queue.get()
                nexus_file.update(event)
                last_update_received[0] = datetime.utcnow()

        coro = consumer(queue, nexus_file)
        try:
            await wait_for(coro, self._timeout)
        except TimeoutError:
            pass

        collector_status.set_collection_time(
            self.name, (last_update_received[0] - first_update_received).total_seconds()
        )

        # When all tasks are done, write the file and send metadata to indexer
        with self._file_lock:
            self._concurrent_events[nexus_file.name].remove(queue)

            if self._concurrent_events[nexus_file.name] == []:
                await nexus_file.index(settings.indexer_url)
                await nexus_file.write()
                self._concurrent_events.pop(nexus_file.name)
                self.discard_file(nexus_file)

    def event_matches(self, event: Event):
        if event.timing_event_code != self.event_code:
            return False
        if event.pv_name not in self.pvs:
            return False
        return True
