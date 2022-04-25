import json
from fastapi import FastAPI

from .events import SDSEvent

app = FastAPI()


def load_events():
    with open("config.json") as config_file:
        for event in json.load(config_file):
            SDSEvent.from_dict(event)


@app.on_event("startup")
async def startup_event():
    load_events()
