from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from elasticsearch import BadRequestError, NotFoundError
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from esds.common.db import settings
from esds.common.db.connection import get_connection

logger = logging.getLogger(__name__)


class Base(BaseModel):
    id: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    @model_validator(mode="before")
    def from_hit(cls, values):
        hit = values.pop("hit", None)
        if not hit:
            return values

        values.update(
            id=hit.pop("_id"),
            **hit.pop("_source"),
        )
        if "@timestamp" in values:
            values.update(timestamp=values.pop("@timestamp"))

        return values

    async def save(self):
        async with get_connection() as es:
            response = await es.index(
                index=self.get_index(),
                document=self.model_dump(exclude_none=True, by_alias=True),
            )
            self.id = response["_id"]
            return self

    async def set_expiry(self, date, expiry_index):
        await expiry_index.create(
            {
                "index": self.get_index(),
                "id": self.id,
                "expire_by": date,
            }
        )

    @classmethod
    def _recursive_mappings(cls, items):
        properties = {}
        for key, fieldinfo in items:
            if key == "id":
                continue
            if issubclass(fieldinfo.annotation, Base):
                properties[key] = cls._recursive_mappings(
                    fieldinfo.annotation.model_fields.items()
                )
            else:
                properties[key] = {"type": fieldinfo.annotation.es_type}

        return {"properties": properties}

    @classmethod
    def mappings(cls):
        return cls._recursive_mappings(cls.model_fields.items())

    @classmethod
    async def init(cls):
        async with get_connection() as es:
            try:
                await es.indices.create(
                    index=cls.get_index(),
                    mappings=cls.mappings(),
                )
                logger.info(f"ES index '{cls.get_index()}' created")
            except BadRequestError:
                logger.info(f"ES index '{cls.get_index()}' already exists")

    @classmethod
    async def get(cls, id):
        async with get_connection() as es:
            try:
                response = await es.search(
                    index=cls.get_index(),
                    query={"ids": {"values": [id]}},
                )
                if response["hits"]["hits"] == []:
                    return None
                return cls(hit=response["hits"]["hits"][0])
            except NotFoundError:
                return None

    @classmethod
    async def mget(
        cls,
        *,
        filters: Optional[List[dict]] = None,
        query: Optional[dict] = None,
        script: Optional[dict] = None,
        sort: Optional[dict] = None,
        search_after: Optional[int] = None,
        size: Optional[int] = None,
    ) -> Tuple[int, List[Base], int]:
        if not query and filters:
            query = {"bool": {"must": list(filters)}}
            if script:
                query["bool"]["filter"] = {"script": {"script": script}}

        async with get_connection() as es:
            try:
                if search_after is not None:
                    search_after = [search_after]

                if size is None:
                    size = settings.max_query_size

                response = await es.search(
                    index=cls.get_index(),
                    query=query,
                    size=size,
                    sort=sort,
                    search_after=search_after,
                )
            except NotFoundError:
                return (0, [], None)

            n_total = response["hits"]["total"]["value"]

            search_after = None

            # Query got 0 hits, either in total or after paginating with search_after
            if n_total == 0 or response["hits"]["hits"] == []:
                return (n_total, [], None)

            hits = list(map(lambda hit: cls(hit=hit), response["hits"]["hits"]))

            if sort is not None:
                search_after = response["hits"]["hits"][-1]["sort"][0]

            return n_total, hits, search_after

    @classmethod
    async def get_aggs(
        cls,
        *,
        filters: Optional[List[dict]] = None,
        query: Optional[dict] = None,
        script: Optional[dict] = None,
        aggs: Optional[dict] = None,
        sort: Optional[dict] = None,
    ):
        if not query and filters:
            query = {"bool": {"must": list(filters)}}
            if script:
                query["bool"]["filter"] = {"script": {"script": script}}

        async with get_connection() as es:
            try:

                response = await es.search(
                    index=cls.get_index(),
                    query=query,
                    size=0,  # We don't need the hits, only the aggregations
                    sort=sort,
                    aggs=aggs,
                )
            except NotFoundError:
                return []

            return response

    @classmethod
    async def create(cls: Base, dict):
        try:
            db_obj = cls.model_validate(dict)
        except ValidationError:
            raise

        await db_obj.save()
        return db_obj

    @classmethod
    def get_index(cls):
        return cls.index

    @classmethod
    async def refresh_index(cls):
        async with get_connection() as es:
            return await es.indices.refresh(index=cls.index)
