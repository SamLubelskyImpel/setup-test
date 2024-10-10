"""Lead Model."""

from sqlalchemy import Column, ForeignKey, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from typing import Dict, Any
from crm_orm.session_config import BaseForModels
from crm_orm.models.consumer import Consumer


class Lead(BaseForModels):   # type: ignore
    """Lead Model."""

    __tablename__ = "crm_lead"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_lead_id = Column(String)
    consumer_id = Column(Integer, ForeignKey("crm_consumer.id"))
    consumer = relationship(Consumer, backref="leads")

    lead_ts = Column(DateTime)
    status = Column(String)
    substatus = Column(String)
    comment = Column(String)
    origin_channel = Column(String)
    source_channel = Column(String)
    source_detail = Column(String)
    request_product = Column(String)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))  # type: ignore
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)
    source_system = Column(String)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
