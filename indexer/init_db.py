from typing import List

import logging
from pydantic import parse_file_as

from config import settings
from common import crud, schemas
from common.logger import logger
from common.models import Collector, Dataset, Expiry

models = [
    Collector,
    Dataset,
    Expiry,
]


async def init_db():
    logging.info("Initialising models...")
    for model in models:
        await model.init()

    logging.info("Loading collector definitions...")
    path = settings.collector_definitions
    for collector in parse_file_as(List[schemas.CollectorCreate], path):
        logging.info(f"Collector '{collector.name}' loaded from file")
        filters = [
            {"match": {"name": collector.name}},
            {"match": {"event_name": collector.event_name}},
            {"match": {"event_code": collector.event_code}},
        ]
        for pv in collector.pvs:
            filters.append({"match": {"pvs": pv}})
        matching_collectors = await crud.collector.get_multi(filters=filters)
        if len(matching_collectors) > 0:
            logging.info(f"Collector '{collector.name}' already in DB")
        else:
            await crud.collector.create(obj_in=collector)
            logging.info(f"Collector '{collector.name}' created in DB")
