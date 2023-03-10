from sds.common.crud.base import CRUDBase
from sds.common.models.collector import Collector
from sds.common.schemas.collector import CollectorCreate


class CRUDCollector(CRUDBase[Collector, CollectorCreate]):
    pass


collector = CRUDCollector(Collector)
