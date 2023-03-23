from esds.common.db.base_class import Base
from esds.common.db.fields import Date, Keyword


class Expiry(Base):
    index: Keyword
    id: Keyword
    expire_by: Date

    class Index:
        name = "expiry"
