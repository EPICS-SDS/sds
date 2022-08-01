from typing import List, Optional

from datetime import datetime, timedelta
from pathlib import Path

from common.crud.base import CRUDBase
from common.models import Dataset, Expiry
from common.schemas import DatasetCreate


class CRUDDataset(CRUDBase[Dataset, DatasetCreate]):
    async def create(
        self,
        *,
        ttl: Optional[int] = None,
        obj_in: DatasetCreate,
    ) -> Dataset:
        db_obj = await super().create(obj_in=obj_in)
        if ttl:
            date = datetime.utcnow() + timedelta(seconds=ttl)
            await db_obj.set_expiry(date, Expiry)
        return db_obj

    async def get_multi_by_path(self, *paths: List[Path]) -> List[Dataset]:
        clauses = [{"match": {"path": path}} for path in paths]
        query = {"bool": {"should": clauses}}
        return await self.model.mget(query=query)


dataset = CRUDDataset(Dataset)
