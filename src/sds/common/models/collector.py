from sds.common.db.base_class import Base
from sds.common.db.fields import Date, Integer, Keyword


class Collector(Base):
    name: Keyword
    pvs: Keyword
    event_name: Keyword
    event_code: Integer
    host: Keyword
    created: Date

    class Index:
        name = "collector"
