from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from pydantic import BaseModel

from esds.common.db.base_class import Base

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        self.model = model

    async def get(self, id: Any) -> Optional[ModelType]:
        return await self.model.get(id)

    async def get_multi(
        self,
        *,
        filters: Optional[List[Dict]] = None,
        script: Optional[List[Dict]] = None,
        sort: Optional[Dict] = None,
        search_after: Optional[int] = None,
        size: Optional[int] = None,
    ) -> Tuple[int, List[ModelType], int]:
        return await self.model.mget(
            filters=filters, script=script, sort=sort, search_after=search_after, size=size,
        )

    async def create(self, obj_in: CreateSchemaType) -> ModelType:
        return await self.model.create(obj_in.model_dump())

    async def refresh_index(self):
        return await self.model.refresh_index()
