from fastapi import FastAPI

from app.logger import logger
from app.api.api import api_router
from app.collector_manager import CollectorManager
from app.collector import load_collectors

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    collectors = load_collectors()
    logger.debug("Starting collectors...",)
    cm = CollectorManager(collectors)
    cm.start()

app.include_router(api_router, prefix="/api")
