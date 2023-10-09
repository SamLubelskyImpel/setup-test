"""Activity Model."""

from datetime import datetime
from sqlalchemy.orm import backref, relationship
from crm_orm.models.lead import Lead
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import JSONB


class Activity(BaseForModels):
    """Activity Model."""

    __tablename__ = "crm_activity"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref=backref("activities", lazy="dynamic"))

    activity_type_id = Column(Integer, ForeignKey("crm_activity_type.id"))
    activity_type = relationship(ActivityType, backref=backref("activities", lazy="dynamic"))

    activity_requested_ts = Column(DateTime)
    request_product = Column(String)
    metadata_ = Column("metadata", JSONB)
    notes = Column(String)
    activity_due_ts = Column(DateTime)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    db_update_date = Column(DateTime)
    db_update_role = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
