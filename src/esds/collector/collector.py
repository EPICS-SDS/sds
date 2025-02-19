import logging
import os.path
from asyncio import Queue, Task, TimeoutError, create_task, get_running_loop, wait_for
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set

from pydantic import ConfigDict

from esds.collector import collector_status
from esds.collector.config import settings
from esds.common.files import Event, NexusFile
from esds.common.files import settings as file_settings
from esds.common.files import write_to_file
from esds.common.schemas import CollectorBase

logger = logging.getLogger(__name__)


class Collector(CollectorBase):
    _tasks: Set[Task] = set()
    _queues: Dict[int, Queue] = dict()
    _events_per_file: int = settings.events_per_file
    _file_lock: Lock
    _files: Dict[int, NexusFile] = dict()
    # To keep track of different datasets stored in the same NeXus file
    _concurrent_datasets: Dict[str, List[Queue]] = dict()
    _pool: ProcessPoolExecutor

    model_config = ConfigDict(frozen=True)

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
            logger.warning(f"{repr(self)} received bad event {repr(event)}")
            return

        with self._file_lock:
            nexus_file = self.get_file(event.sds_event_cycle_id)

            if nexus_file is None:
                # File name is build from the collector parent path, collector name, the event code, and the SDS cycle ID of the first event
                collector_w_path = os.path.join(self.parent_path, self.name)
                file_name: str = f"{collector_w_path.lstrip('/').replace('/','_')}_{str(event.timing_event_code)}_{str(event.sds_event_cycle_id)}"
                # Path is generated from date
                directory = Path(
                    event.sds_event_timestamp.strftime("%Y"),
                    event.sds_event_timestamp.strftime("%Y-%m-%d"),
                )

                # First create a new file
                nexus_file = NexusFile(
                    collector_id=self.collector_id,
                    collector_name=self.name,
                    parent_path=self.parent_path,
                    file_name=file_name,
                    directory=directory,
                )

                absolute_path = file_settings.storage_path / nexus_file.path
                if os.path.exists(absolute_path):
                    logger.error(
                        f"File {nexus_file.path} already exists. Discarding update for SDS event {event}."
                    )
                    return

                # TODO: check that the file is not indexed! When writing multiple SDS events to the same file, checking that the file exists only works for events corresponding to the first SDS event in the file.

                self._files[event.sds_event_cycle_id] = nexus_file
                self._concurrent_datasets[nexus_file.file_name] = 0

            # One queue per sds_event_id
            queue = self._queues.get(event.sds_event_cycle_id)
            if queue is None:
                queue = self.create_queue(event.sds_event_cycle_id)

                self._concurrent_datasets[nexus_file.file_name] += 1

                task = create_task(self._collector(queue, nexus_file))
                self._tasks.add(task)

                def task_done_cb(task: Task):
                    try:
                        task.result()
                    except Exception as e:
                        logger.error(
                            f"{repr(self)} Exception running the _collector() task for file {nexus_file}."
                        )
                        logger.error(e)

                    self._tasks.discard(task)
                    self.discard_queue(event.sds_event_cycle_id)

                task.add_done_callback(task_done_cb)

        # Update the timestamp of the start of the cycle when the SDS event happened
        if event.cycle_id == event.sds_event_cycle_id:
            event.sds_cycle_start_timestamp = event.cycle_id_timestamp

        queue.put_nowait(event)

        collector_status.set_event_size(
            self.collector_id, event.pv_name, event.event_size
        )
        collector_status.set_update_event(
            self.collector_id, event.pv_name, event.sds_event_cycle_id
        )

    async def _collector(self, queue: Queue, nexus_file: NexusFile):
        first_update_received = datetime.now(UTC)
        last_flush = datetime.now(UTC)
        last_update_received = datetime.now(UTC)

        while (
            not queue.empty()
            or (datetime.now(UTC) - last_update_received).total_seconds()
            < settings.collector_timeout
        ):
            try:
                event = await wait_for(queue.get(), settings.flush_file_delay)
                async with nexus_file.lock:
                    nexus_file.add_event(event)
                last_update_received = datetime.now(UTC)
            except TimeoutError:
                pass

            # Flush file at regular intervals to free memory
            if (
                datetime.now(UTC) - last_flush
            ).total_seconds() > settings.flush_file_delay:
                last_flush = datetime.now(UTC)
                await self._flush_events_to_file(nexus_file)

            with self._file_lock:
                # When the collector task times out, new events will be discarded
                if (
                    datetime.now(UTC) - last_update_received
                ).total_seconds() > settings.collector_timeout:
                    # Stop the collector task (this method) for this dataset corresponding to an sds_event_id
                    self._concurrent_datasets[nexus_file.file_name] -= 1
                    file_ready = self._concurrent_datasets[nexus_file.file_name] == 0
                    if file_ready:
                        self._concurrent_datasets.pop(nexus_file.file_name)
                        self.discard_file(nexus_file)
                        # At this point, any new event received in the update() method with this sds_event_id will be discarded

        # Making sure the queue is empty before closing the collector
        await self._flush_events_to_file(nexus_file)

        # Push metadata to the indexer service
        await nexus_file.index(settings.indexer_url)

        # Update the collector status information
        collector_status.set_last_collection(
            self.collector_id, event.sds_event_cycle_id
        )
        collector_status.set_collection_time(
            self.collector_id,
            (last_update_received - first_update_received).total_seconds(),
        )

        collector_status.remove_sds_cycle(self.collector_id, event.sds_event_cycle_id)

        if file_ready:
            collector_status.set_collection_size(
                self.collector_id, nexus_file.getsize()
            )

    async def _flush_events_to_file(self, nexus_file: NexusFile):
        async with nexus_file.lock:
            if len(nexus_file.events) != 0:
                try:
                    write_result = await get_running_loop().run_in_executor(
                        self._pool, write_to_file, nexus_file
                    )

                    # Update the PV status information (here all events share the same SDS cycle ID)
                    if write_result != {}:
                        collector_status.update_written_data(
                            write_result,
                            self.collector_id,
                            nexus_file.events[0].sds_event_cycle_id,
                        )

                    # Clearing the events because the executor works on a copy of the list
                    nexus_file.clear_events()
                except Exception as e:
                    logger.error(f"Exception while writing to file {nexus_file}.", e)

    def event_matches(self, event: Event):
        if event.timing_event_code != self.event_code:
            return False
        if event.pv_name not in self.pvs:
            return False
        return True
