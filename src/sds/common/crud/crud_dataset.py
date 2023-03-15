from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from sds.common.crud.base import CRUDBase
from sds.common.models import Dataset, Expiry
from sds.common.schemas import DatasetCreate


class CRUDDataset(CRUDBase[Dataset, DatasetCreate]):
    async def create(
        self,
        obj_in: DatasetCreate,
        *,
        ttl: Optional[int] = None,
    ) -> Dataset:
        db_obj = await super().create(obj_in=obj_in)
        if ttl:
            date = datetime.utcnow() + timedelta(seconds=ttl)
            await db_obj.set_expiry(date, Expiry)
        return db_obj

    async def get_multi_by_path(
        self, *paths: List[Path]
    ) -> Tuple[int, List[Dataset], int]:
        clauses = [{"match": {"path": str(path)}} for path in paths]
        query = {"bool": {"should": clauses}}
        return await self.model.mget(query=query)


dataset = CRUDDataset(Dataset)