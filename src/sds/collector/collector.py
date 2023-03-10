from asyncio import Queue, Task, TimeoutError, create_task, get_running_loop, wait_for
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set

from sds.collector import collector_status
from sds.collector.config import settings
from sds.common.files import Event, NexusFile, write_file
from sds.common.schemas import CollectorBase


class Collector(CollectorBase):
    id: str
    _tasks: Set[Task] = set()
    _queues: Dict[int, Queue] = dict()
    _timeout: int = settings.collector_timeout
    _events_per_file: int = settings.events_per_file
    _file_lock: Lock
    _files: Dict[int, NexusFile] = dict()
    _concurrent_events: Dict[str, List[Queue]] = dict()
    _pool: ProcessPoolExecutor

    class Config:
        frozen = True
        underscore_attrs_are_private = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._file_lock: Lock = Lock()

    def get_file(self, id: int) -> NexusFile:
        for f in self._files.keys():
            if id >= f and id < f + self._events_per_file:
                return self._files[f]
        return None

    def discard_file(self, f: NexusFile):
        for key, value in dict(self._files).items():
            if value == f:
                del self._files[key]

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
            nexus_file = self.get_file(event.sds_event_pulse_id)

            if nexus_file is None:
                # File name is build from the collector name, the event code, and the pulse ID of the first event
                file_name: str = f"{self.name}_{str(event.timing_event_code)}_{str(event.sds_event_pulse_id)}"
                # Path is generated from date
                directory = Path(
                    event.sds_event_timestamp.strftime("%Y"),
                    event.sds_event_timestamp.strftime("%Y-%m-%d"),
                )

                # First create a new file
                nexus_file = NexusFile(
                    collector_id=self.id,
                    collector_name=self.name,
                    file_name=file_name,
                    directory=directory,
                )
                self._files[event.sds_event_pulse_id] = nexus_file
                self._concurrent_events[nexus_file.file_name] = []

            # One queue per sds_event_id
            queue = self._queues.get(event.sds_event_pulse_id)
            if queue is None:
                queue = self.create_queue(event.sds_event_pulse_id)

                self._concurrent_events[nexus_file.file_name].append(queue)

                task = create_task(self._collector(queue, nexus_file))
                self._tasks.add(task)

                def task_done_cb(task):
                    self._tasks.discard(task)
                    self.discard_queue(event.sds_event_pulse_id)

                task.add_done_callback(task_done_cb)

        queue.put_nowait(event)

    async def _collector(self, queue: Queue, nexus_file: NexusFile):
        first_update_received = datetime.utcnow()
        last_update_received = [datetime.utcnow()]

        async def consumer(queue: Queue, nexus_file: NexusFile):
            while True:
                event = await queue.get()
                nexus_file.add_event(event)
                last_update_received[0] = datetime.utcnow()

        coro = consumer(queue, nexus_file)
        try:
            await wait_for(coro, self._timeout)
        except TimeoutError:
            pass

        collector_status.set_last_collection(self.name)
        collector_status.set_collection_time(
            self.name, (last_update_received[0] - first_update_received).total_seconds()
        )

        # When all tasks are done, write the file and send metadata to indexer
        with self._file_lock:
            self._concurrent_events[nexus_file.file_name].remove(queue)

            file_ready = self._concurrent_events[nexus_file.file_name] == []
            if file_ready:
                self._concurrent_events.pop(nexus_file.file_name)
                self.discard_file(nexus_file)

        if file_ready:
            await get_running_loop().run_in_executor(self._pool, write_file, nexus_file)
            await nexus_file.index(settings.indexer_url)
            collector_status.set_collection_size(self.name, nexus_file.getsize())

    def event_matches(self, event: Event):
        if event.timing_event_code != self.event_code:
            return False
        if event.pv_name not in self.pvs:
            return False
        return True
