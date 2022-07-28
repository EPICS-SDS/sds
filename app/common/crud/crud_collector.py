from common.crud.base import CRUDBase
from common.models.collector import Collector
from common.schemas.collector import CollectorCreate


class CRUDCollector(CRUDBase[Collector, CollectorCreate]):
    pass


collector = CRUDCollector(Collector)
