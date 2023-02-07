from datetime import datetime
from pathlib import Path

import aiohttp
from pydantic import BaseModel


class Dataset(BaseModel):
    """
    Model for a file containing Event objects that belong to the same timing
    event (same trigger_pulse_id) and share the same collector.
    """

    collector_id: str
    trigger_date: datetime
    trigger_pulse_id: int
    path: Path

    async def index(self, indexer_url):
        """
        Publish metadata into the indexer service
        """
        try:
            url = indexer_url + "/datasets"
            data = Dataset.parse_obj(self).json()
            headers = {"Content-Type": "application/json"}
            async with aiohttp.ClientSession(headers=headers) as client:
                async with client.post(url, data=data) as response:
                    response.raise_for_status()
        except Exception as e:
            print(repr(self), "indexing failed!")
            print(e)
