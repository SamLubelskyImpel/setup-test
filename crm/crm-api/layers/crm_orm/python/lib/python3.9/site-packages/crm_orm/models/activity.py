"""Activity Model."""

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from typing import Dict, Any
from crm_orm.session_config import BaseForModels
from crm_orm.models.activity_type import ActivityType
from crm_orm.models.lead import Lead


class Activity(BaseForModels):  # type: ignore
    """Activity Model."""

    __tablename__ = "crm_activity"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref="activities")

    activity_type_id = Column(Integer, ForeignKey("crm_activity_type.id"))
    activity_type = relationship(ActivityType)

    crm_activity_id = Column(String)
    activity_requested_ts = Column(DateTime)
    request_product = Column(String)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))  # type: ignore
    notes = Column(String)
    contact_method = Column(String)
    activity_due_ts = Column(DateTime)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
