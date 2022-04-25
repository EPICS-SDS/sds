from fastapi import APIRouter

from .endpoints import events

api_router = APIRouter()
api_router.include_router(events.router, prefix="/events")
