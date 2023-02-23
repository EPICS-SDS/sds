from collections import deque
from datetime import datetime
from typing import Dict, List, Optional

from common.schemas import CollectorBase
from pydantic import BaseModel


class Settings:
    def __init__(self):
        self.collectors: Dict[str, CollectorBase] = dict()


class PvStatusSchema(BaseModel):
    name: str
    last_event: Optional[datetime] = None
    connected: bool = False
    event_rate: float = 0
    event_loss_rate: float = 0


class CollectorBasicStatus(BaseModel):
    name: str
    running: bool = False
    last_collection: Optional[datetime] = None
    # Time it takes to collect all PVs for one event. It should sufficiently lower than the `collector_timeout` setting.
    collection_time: float = 0


class CollectorFullStatus(CollectorBasicStatus):
    pvs: List[PvStatusSchema]


class CollectorStatus(CollectorFullStatus):
    collection_time_queue: deque[float] = deque(maxlen=5)


class PvStatus:
    def __init__(self, name: str):
        self.event_timestamps: deque[datetime] = deque(maxlen=5)
        self.pv_status = PvStatusSchema(name=name)

    def set_last_event(self, last_event: datetime):
        self.pv_status.last_event = last_event
        if last_event is not None:
            self.event_timestamps.append(last_event)
            if len(self.event_timestamps) > 1:
                self.pv_status.event_rate = (len(self.event_timestamps) - 1) / (
                    self.event_timestamps[-1] - self.event_timestamps[0]
                ).total_seconds()


class StatusManager:
    def __init__(self):
        self.collector_status_dict: Dict[str, CollectorStatus] = dict()
        self.pv_status_dict = dict()

    def add_collector(self, collector: CollectorBase):
        for pv in collector.pvs:
            if pv not in self.pv_status_dict.keys():
                self.pv_status_dict[pv] = PvStatus(name=pv)
        self.collector_status_dict.update(
            {
                collector.name: CollectorStatus(
                    name=collector.name,
                    running=False,
                    pvs=[
                        pvstatus.pv_status
                        for pvname, pvstatus in self.pv_status_dict.items()
                        if pvname in collector.pvs
                    ],
                )
            }
        )

    def remove_collector(self, collector_name: str):
        collector_rm = self.collector_status_dict.pop(collector_name)

        pvs_to_keep = {
            pv.name
            for collector in self.collector_status_dict.values()
            for pv in collector.pvs
        }
        for pv in collector_rm.pvs:
            if pv.name not in pvs_to_keep:
                self.pv_status_dict.pop(pv.name)

    def get_pv_status(self, pv: str) -> PvStatus:
        return self.pv_status_dict[pv].pv_status

    def set_update_event(self, pv: str):
        self.pv_status_dict[pv].set_last_event(datetime.utcnow())

    def set_collector_running(self, collector_name: str, running: bool):
        collector = self.collector_status_dict.get(collector_name)
        collector.running = running

    def set_last_collection(self, collector_name: str):
        collector = self.collector_status_dict.get(collector_name)
        collector.last_collection = datetime.utcnow()

    def set_collection_time(self, collector_name: str, collection_time: float):
        collector = self.collector_status_dict.get(collector_name)
        collector.collection_time_queue.append(collection_time)
        collector.collection_time = sum(collector.collection_time_queue) / len(
            collector.collection_time_queue
        )
