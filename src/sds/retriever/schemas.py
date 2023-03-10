from typing import List, Optional

from pydantic import BaseModel
from sds.common import schemas


class MultiResponse(BaseModel):
    total: int
    search_after: Optional[int]


class MultiResponseDataset(MultiResponse):
    datasets: List[schemas.Dataset]


class MultiResponseCollector(MultiResponse):
    collectors: List[schemas.Collector]
