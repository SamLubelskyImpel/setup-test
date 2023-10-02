"""Activity Model."""

from datetime import datetime
from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import JSONB


class Activity(BaseForModels):
    """Activity Model."""

    __tablename__ = "crm_activity"

    id = Column(Integer, primary_key=True)
    integration_partner_id = Column(Integer, ForeignKey("crm_integration_partner.id"))
    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    dealer_id = Column(Integer, ForeignKey("crm_dealer.id"))
    activity_type_id = Column(Integer, ForeignKey("crm_activity_type.id"))
    activity_requested_ts = Column(DateTime)
    request_product = Column(Integer)
    metadata = Column(JSONB)
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
