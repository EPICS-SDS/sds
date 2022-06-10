from typing import List, Optional, Set

from threading import Timer
from pydantic import BaseModel, Field, parse_file_as
import requests

from app.logger import logger
from app.config import settings
from app.dataset import Dataset
from app.event import Event


class Collector(BaseModel):
    id: Optional[str]
    name: str
    pvs: Set[str] = Field(alias="pv_list")
    event_name: str
    event_code: int
    _dataset: Optional[Dataset] = None
    _timer: Optional[Timer] = None

    class Config:
        # arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def register(self):
        url = f"{settings.indexer_url}/get_collector"
        params = {
            "collector_name": self.name,
            "event_name": self.event_name,
            "event_code": self.event_code,
            "pv_list": list(self.pvs),
        }
        response = requests.post(url, params=params)
        response.raise_for_status()
        data = response.json()
        self.id = data["id"]
        logger.debug(f"Collector '{self.name}' registered as '{self.id}'")

    def update(self, event: Event):
        # If the PV does not belong to this event, ignore it.
        if event.pv_name not in self.pvs:
            return
        # Start a timer to call a function to write values after a given time
        # if not all PVs have been received.
        if self._dataset is None:
            self._dataset = Dataset(name=self.name)

            self._timer = Timer(settings.collector_timeout, self.timeout)
            self._timer.start()

        self._dataset.update(event)
        if self.is_dataset_complete(self._dataset):
            self.finish()

    def is_dataset_complete(self, dataset: Dataset):
        return all(map(lambda d: set(d) == self.pvs, dataset.entry))

    def timeout(self):
        logger.debug(f"Collector '{self.name}' timed out")
        self.finish()

    def finish(self):
        self._timer.cancel()
        logger.debug(f"Collector '{self.name}' done")
        self._dataset.write()
        self._dataset = None


def load_collectors():
    logger.debug("Loading collector definitions...")
    collectors = []
    path = settings.collector_definitions
    for collector in parse_file_as(List[Collector], path):
        logger.debug(f"Collector '{collector.name}' loaded")
        try:
            collector.register()
            collectors.append(collector)
        except Exception as e:
            logger.warning(e)
            logger.warning(f"Collector '{collector.name}' failed to register")
    return collectors
