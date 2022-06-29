from datetime import datetime
from pathlib import Path
import aiohttp
from nexusformat.nexus import NXdata, NXentry
from pydantic import BaseModel, root_validator

from app.config import settings
from app.event import Event


class DatasetSchema(BaseModel):
    collector_id: str
    trigger_date: datetime
    trigger_pulse_id: int
    path: Path


class Dataset(DatasetSchema):
    name: str
    entry: NXentry

    class Config:
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def extract_path(cls, values):
        timestamp = values["trigger_date"].strftime("%Y%m%d_%H%M%S")
        name = f"{values['collector_name']}_{timestamp}"
        values.update(name=name)

        directory = Path(values["trigger_date"].strftime("%Y"),
                         values["trigger_date"].strftime("%Y-%m-%d"))
        path = directory / f"{name}.h5"
        values.update(path=path)

        entry = NXentry(attrs={
            "collector_name": values["collector_name"],
            "event_name": values["event_name"],
            "event_code": values["event_code"],
        })
        values.update(entry=entry)
        return values

    def update(self, event: Event):
        key = f"event_{event.pulse_id}"
        if key not in self.entry:
            self.entry[key] = NXdata(attrs={"pulse_id": event.pulse_id})
        self.entry[key][event.pv_name] = event.value

    async def write(self):
        try:
            print(f"Dataset '{self.name}' writing to '{self.path}'")
            absolute_path = settings.storage_path / self.path
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            self.entry.save(absolute_path)
            print(f"Dataset '{self.name}' writing done.")
        except Exception as e:
            print(f"Dataset '{self.name}' writing failed!")
            print(e)

    async def upload(self):
        try:
            print(f"Dataset '{self.name}' indexing...")
            url = settings.indexer_url + "/datasets"
            data = DatasetSchema.parse_obj(self).json()
            async with aiohttp.ClientSession() as client:
                async with client.post(url, json=data) as response:
                    response.raise_for_status()
            print(f"Dataset '{self.name}' indexing done.")
        except Exception as e:
            print(f"Dataset '{self.name}' indexing failed!")
            print(e)
