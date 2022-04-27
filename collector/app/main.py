import json
from fastapi import FastAPI

from .logger import logger
from .events import SDSEvent
from .api.api import api_router

app = FastAPI()


def load_events():
    with open("../config.json") as config_file:
        for event in json.load(config_file):
            SDSEvent.from_dict(event)
            logger.debug("Event '%s' loaded", event)


@app.on_event("startup")
async def startup_event():
    load_events()

app.include_router(api_router, prefix="/api")
