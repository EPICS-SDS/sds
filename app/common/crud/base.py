from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel

from common.db.base_class import Base


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
    ) -> List[ModelType]:
        return await self.model.mget(filters=filters)

    async def create(self, obj_in: CreateSchemaType) -> ModelType:
        return await self.model.create(obj_in.dict(by_alias=True))
