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
                logger.warning("PV %s error: %s", pv, value)
            elif "eventName" in value.attrib:
                pulse_id = value.raw["timeStamp.userTag"]
                event_name = value.attrib["eventName"]
                logger.debug("PV %s received event '%s' (%d)",
                             pv, event_name, pulse_id)
                self.event_handler(event_name, pulse_id, pv, value)
        return cb

    # Finds the event and updates it with the value
    def event_handler(self, name, pulse_id, pv, value):
        event = SDSEvent.get(name)
        if event is None:
            logger.info("Event %s unknown", name)
        else:
            event.update(pulse_id, pv, value)
