from common.db.base_class import Base
from common.db.fields import Date, Integer, Text


class Dataset(Base):
    collector_id: Text
    trigger_pulse_id: Integer
    path: Text
    created: Date

    class Index:
        name = "dataset"
