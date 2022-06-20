from common.db.base_class import Base
from common.db.fields import Date, Integer, Text


class Collector(Base):
    name: Text
    pvs: Text
    event_name: Text
    event_code: Integer
    created: Date

    class Index:
        name = "collector"
