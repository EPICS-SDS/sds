import json
from fastapi import FastAPI

from .logger import logger
from .events import SDSEvent
from .api.api import api_router
from .pv_event_monitor import PVEventMonitor

app = FastAPI()


def load_events():
    with open("../config.json") as config_file:
        for event in json.load(config_file):
            SDSEvent.from_dict(event)
            logger.debug("Event '%s' loaded", event)


@app.on_event("startup")
async def startup_event():
    load_events()

    pem = PVEventMonitor()
    pem.start()

app.include_router(api_router, prefix="/api")
