from typing import ClassVar

from esds.common.db.base_class import Base
from esds.common.db.fields import Date, Integer, Keyword


class Collector(Base):
    name: Keyword
    pvs: Keyword
    event_name: Keyword
    event_code: Integer
    host: Keyword
    created: Date

    index: ClassVar[str] = "collector"
