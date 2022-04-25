import json
from fastapi import FastAPI

app = FastAPI()


def load_events():
    with open("config.json") as config_file:
        for event in json.load(config_file):
            print(event)


@app.on_event("startup")
async def startup_event():
    load_events()
