from sds.common.db.base_class import Base
from sds.common.db.fields import Date, Keyword


class Expiry(Base):
    index: Keyword
    id: Keyword
    expire_by: Date

    class Index:
        name = "expiry"
