from typing import List
from p4p.client.asyncio import Context

from app.logger import logger
from app.collector import Collector
from app.event import Event


class CollectorManager:
    def __init__(self, collectors: List[Collector]):
        self.context = Context("pva")
        self.collectors = list(collectors)

    # Start monitoring each PV for the events
    def start(self):
        pvs = set()
        for collector in self.collectors:
            pvs |= collector.pvs

        self.subs = set()
        for pv in pvs:
            logger.debug("PV %s is being monitored", pv)
            sub = self.context.monitor(
                pv, self.pv_handler(pv), notify_disconnect=True)
            self.subs.add(sub)

    # Factory function to create a callback for a PV monitor
    def pv_handler(self, pv: str):
        async def cb(value):
            if isinstance(value, Exception):
                logger.warning(f"PV {pv} error: {value}")
                return
            try:
                event = Event(pv_name=pv, value=value)
            except ValidationError:
                logger.warning(f"PV {pv} validation error")
                return
            logger.debug(f"PV {pv} received event "
                         f"'{event.name}' ({event.pulse_id})")
            self.event_handler(event)
        return cb

    # Finds the event and updates it with the value
    def event_handler(self, event: Event):
        collector = next(
            (c for c in self.collectors if c.name == event.name), None)
        if collector is None:
            logger.debug("Collector %s unknown", event.name)
            return
        collector.update(event)
