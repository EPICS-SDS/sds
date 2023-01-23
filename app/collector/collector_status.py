from collections import deque
from datetime import datetime
from typing import List, Optional, Set

from common.schemas import CollectorBase
from pydantic import BaseModel


class Settings:
    def __init__(self):
        self.collectors: Set[CollectorBase] = set()


class PvStatusSchema(BaseModel):
    name: str
    last_event: Optional[datetime] = None
    connected: bool = False
    event_rate: float = 0
    event_loss_rate: float = 0


class CollectorBasicStatus(BaseModel):
    name: str
    running: bool = False
    last_event: Optional[datetime] = None


class CollectorStatus(CollectorBasicStatus):
    pvs: List[PvStatusSchema]


class StatusManager:
    def __init__(self):
        self.collector_status: List[CollectorStatus] = []
        self.pvs = dict()

    @property
    def collector_status(self):
        for collector in self.__collector_status:
            for pv in collector.pvs:
                if pv.last_event is not None and (
                    collector.last_event is None or pv.last_event > collector.last_event
                ):
                    collector.last_event = pv.last_event
        return self.__collector_status

    @collector_status.setter
    def collector_status(self, collector_status):
        self.__collector_status = collector_status

    def add_collector(self, collector: CollectorBase):
        for pv in collector.pvs:
            if pv not in self.pvs.keys():
                self.pvs[pv] = PvStatus(name=pv)
        self.__collector_status.append(
            CollectorStatus(
                name=collector.name,
                running=True,
                pvs=[
                    pvstatus.pv_status
                    for pvname, pvstatus in self.pvs.items()
                    if pvname in collector.pvs
                ],
            )
        )

    def set_connected(self, pv: str):
        self.pvs[pv].pv_status.connected = True

    def set_disconnected(self, pv: str):
        self.pvs[pv].pv_status.connected = False


class PvStatus:
    def __init__(self, name: str):
        self.event_timestamps: deque[datetime] = deque(maxlen=5)
        self.pv_status = PvStatusSchema(name=name)

    @property
    def last_event(self):
        return self.pv_status.last_event

    @last_event.setter
    def last_event(self, last_event: datetime):
        self.pv_status.last_event = last_event
        if last_event is not None:
            self.event_timestamps.append(last_event)
            if len(self.event_timestamps) > 1:
                self.pv_status.event_rate = (len(self.event_timestamps) - 1) / (
                    self.event_timestamps[-1] - self.event_timestamps[0]
                ).total_seconds()
