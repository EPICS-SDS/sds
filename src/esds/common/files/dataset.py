import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Dataset(BaseModel):
    """
    Model for a file containing Event objects that belong to the same timing
    event (same sds_event_cycle_id) and share the same collector.
    """

    collector_id: str
    sds_event_timestamp: datetime
    sds_cycle_start_timestamp: datetime
    sds_event_cycle_id: int
    path: Path
    beam_info: Optional[Dict[str, Any]]

    async def index(self, indexer_url):
        """
        Publish metadata into the indexer service
        """
        try:
            url = urljoin(str(indexer_url), "/datasets")
            data = Dataset.model_validate(self).model_dump_json()
            headers = {"Content-Type": "application/json"}
            async with aiohttp.ClientSession(headers=headers) as client:
                async with client.post(url, data=data) as response:
                    response.raise_for_status()
        except Exception as e:
            logger.error(f"{repr(self)} indexing failed!")
            logger.error(e)
