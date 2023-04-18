import logging

from esds.common.models import Collector, Dataset, Expiry

logger = logging.getLogger(__name__)


models = [
    Collector,
    Dataset,
    Expiry,
]


async def init_db():
    logger.info("Initialising models...")
    for model in models:
        await model.init()
