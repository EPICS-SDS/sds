import logging

from esds.common.models import Collector, Dataset, Expiry

models = [
    Collector,
    Dataset,
    Expiry,
]


async def init_db():
    logging.info("Initialising models...")
    for model in models:
        await model.init()
