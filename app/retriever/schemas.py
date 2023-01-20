from typing import List, Optional

from common import schemas
from pydantic import BaseModel


class MultiResponse(BaseModel):
    total: int
    search_after: Optional[int]


class MultiResponseDataset(MultiResponse):
    datasets: List[schemas.Dataset]


class MultiResponseCollector(MultiResponse):
    collectors: List[schemas.Collector]
