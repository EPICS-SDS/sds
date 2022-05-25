from fastapi import APIRouter, HTTPException

from app.events import SDSEvent


router = APIRouter()


@router.get("/")
def read_events():
    events = SDSEvent.get_multi()
    return events


@router.get("/{event_id}")
def read_event(event_id: str):
    event = SDSEvent.get(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event
