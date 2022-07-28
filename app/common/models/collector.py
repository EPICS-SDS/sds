from common.db.base_class import Base
from common.db.fields import Date, Integer, Keyword, Text


class Collector(Base):
    name: Text
    pvs: Keyword
    event_name: Keyword
    event_code: Integer
    created: Date

    class Index:
        name = "collector"
