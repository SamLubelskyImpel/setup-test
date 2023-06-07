"""Integration Partner Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String


class DealerIntegrationPartner(BaseForModels):
    """Dealer Integration Partner Model."""

    __tablename__ = "dealer_integration_partner"

    id = Column(Integer, primary_key=True)
    integration_partner_id = Column(Integer, ForeignKey("integration.id"))
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    dms_id = Column(String)
    is_active = Column(Boolean)
    db_creation_date = Column(DateTime)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
