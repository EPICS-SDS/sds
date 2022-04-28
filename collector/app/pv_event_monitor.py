from p4p.client.asyncio import Context

from .logger import logger
from .events import SDSEvent


class PVEventMonitor:
    def __init__(self):
        self.subs = set()
        self.context = Context("pva")

    # Start monitoring each PV for the events
    def start(self):
        for pv in SDSEvent.get_all_pvs():
            logger.debug("PV %s is being monitored", pv)
            sub = self.context.monitor(
                pv, self.pv_handler(pv), notify_disconnect=True)
            self.subs.add(sub)

    # Factory function to create a callback for a PV monitor
    def pv_handler(self, pv):
        async def cb(value):
            if isinstance(value, Exception):
                pass
                logger.warning("PV %s error: %s", pv, value)
            else:
                logger.debug("PV %s received (%d)", pv,
                             value["timeStamp.userTag"])
                self.value_handler(pv, value)
        return cb

    # Finds each event referenced in the PV
    def value_handler(self, pv, value):
        d = value.todict()
        for item in d["timing"]:
            self.event_handler(item["eventName"], pv, value)

    # Finds the event and updates it with the value
    def event_handler(self, name, pv, value):
        event = SDSEvent.get(name)
        if event is None:
            logger.info("Event %s unknown", name)
        else:
            event.update(value["timeStamp.userTag"], pv, value["value"])
