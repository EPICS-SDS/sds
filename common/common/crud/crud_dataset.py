from typing import Optional

from datetime import datetime, timedelta

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


dataset = CRUDDataset(Dataset)
