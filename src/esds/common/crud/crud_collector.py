from esds.common.crud.base import CRUDBase
from esds.common.models.collector import Collector
from esds.common.schemas.collector import CollectorCreate


class CRUDCollector(CRUDBase[Collector, CollectorCreate]):
    pass


collector = CRUDCollector(Collector)
