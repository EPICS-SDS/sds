from p4p.client.asyncio import Context

from .logger import logger
from .events import SDSEvent


class PVEventMonitor:
    def __init__(self):
        self.subs = set()
        self.context = Context("pva")

    def start(self):
        for pv in SDSEvent.get_all_pvs():
            self.subs.add(self.context.monitor(
                pv, self.pv_handler(pv), notify_disconnect=True))

    def pv_handler(self, pv):
        async def cb(value):
            if isinstance(value, Exception):
                pass
            else:
                self.value_handler(pv, value)
        return cb

    def value_handler(self, pv, value):
        d = value.todict()
        for item in d["timing"]:
            self.event_handler(item["eventName"], pv, value)

    def event_handler(self, name, pv, value):
        event = SDSEvent.get(name)
        if event is not None:
            event.update(value["timeStamp.userTag"], pv, value["value"])
