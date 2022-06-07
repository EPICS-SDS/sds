from fastapi import FastAPI

from .logger import logger
from .api.api import api_router
from .pv_event_monitor import PVEventMonitor

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    logger.info("Starting event monitor...",)
    pem = PVEventMonitor()
    pem.start()

app.include_router(api_router, prefix="/api")
