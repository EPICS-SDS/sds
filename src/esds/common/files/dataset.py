from datetime import datetime
import logging
from pathlib import Path
import aiohttp
from pydantic import BaseModel

from esds.common.files import BeamInfo


class Dataset(BaseModel):
    """
    Model for a file containing Event objects that belong to the same timing
    event (same sds_event_pulse_id) and share the same collector.
    """

    collector_id: str
    sds_event_timestamp: datetime
    sds_event_pulse_id: int
    path: Path
    beam_info: BeamInfo

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
            logging.error(f"{repr(self)} indexing failed!")
            logging.error(e)
