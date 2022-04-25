from datetime import datetime
from fastapi import APIRouter

from app.events import SDSEvent


router = APIRouter()


@router.get("/")
def read_events():
    events = SDSEvent.get_multi()
    return [event.toJSON() for event in events]


@router.get("/{event_id}")
def read_event(event_id: str):
    event = SDSEvent.get(event_id)
    return event.toJSON()


@router.get("/{event_id}/trigger")
async def get_event_monitor(event_id: str):
    event = SDSEvent.get(event_id)
    results = await event.trigger()
    data = {pv: {
        "timestamp": datetime.fromtimestamp(value.timestamp),
        "pulseid": value.raw["dataTimeStamp"]["userTag"],
        "value": value.tolist()
    } for pv, value in results}
    return data
