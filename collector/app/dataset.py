from typing import Optional

from datetime import datetime
from pathlib import Path
from nexusformat.nexus import NXdata, NXentry
from pydantic import BaseModel, Field
import requests

from app.logger import logger
from app.config import settings
from app.event import Event


class Dataset(BaseModel):
    name: str
    date: datetime = Field(default_factory=datetime.utcnow)
    entry: Optional[NXentry]
    path: Optional[Path]

    class Config:
        arbitrary_types_allowed = True

    def update(self, event: Event):
        # Create entry if it does not exist
        if self.entry is None:
            self.entry = NXentry(attrs={
                "dataset_name": self.name,
                "event_name": event.name,
                "event_code": event.code,
            })
        # Add event to entry
        key = f"event_{event.pulse_id}"
        if key not in self.entry:
            self.entry[key] = NXdata(attrs={"pulse_id": event.pulse_id})
        self.entry[key][event.pv_name] = event.value

    def write(self):
        # Create directory
        directory = Path(settings.output_dir) / \
            self.date.strftime("%Y") / self.date.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        # Create filename
        timestamp = self.date.strftime("%Y%m%d_%H%M%S")
        filename = f"{self.name}_{timestamp}.h5"
        self.path = directory / filename
        logger.debug(f"Dataset '{self.name}' writing to '{self.path}'")
        # Write file
        self.entry.save(self.path)
