"""Lead Model."""

from sqlalchemy.orm import backref, relationship
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB


class Lead(BaseForModels):
    """Lead Model."""

    __tablename__ = "crm_lead"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_lead_id = Column(String)
    consumer_id = Column(Integer, ForeignKey("crm_consumer.id"))
    consumer = relationship(Consumer, backref=backref("leads", lazy="dynamic"))

    lead_ts = Column(DateTime)
    status = Column(String)
    substatus = Column(String)
    comment = Column(String)
    origin_channel = Column(String)
    source_channel = Column(String)
    request_product = Column(String)
    metadata_ = Column("metadata", JSONB)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
