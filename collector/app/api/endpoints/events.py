from fastapi import APIRouter

from ...events import SDSEvent


router = APIRouter()


@router.get("/")
def read_events():
    events = SDSEvent.get_multi()
    return [event.toJSON() for event in events]


@router.get("/{event_id}")
def read_event(event_id: str):
    event = SDSEvent.get(event_id)
    return event.toJSON()
