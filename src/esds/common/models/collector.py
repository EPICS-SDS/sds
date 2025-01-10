from typing import ClassVar

from esds.common.db.base_class import Base
from esds.common.db.fields import Date, Integer, Keyword


class Collector(Base):
    name: Keyword
    pvs: Keyword
    event_code: Integer
    parent_path: Keyword
    created: Date
    collector_id: Keyword
    version: Integer

    index: ClassVar[str] = "collector"
