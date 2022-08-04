from common.db.base_class import Base
from common.db.fields import Date, Integer, Keyword


class Dataset(Base):
    collector_id: Keyword
    trigger_date: Date
    trigger_pulse_id: Integer
    path: Keyword
    created: Date

    class Index:
        name = "dataset"
