"""Dealer Model."""

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import backref, relationship
from typing import Dict, Any
from crm_orm.session_config import BaseForModels
from crm_orm.models.integration_partner import IntegrationPartner


class Dealer(BaseForModels):  # type: ignore
    """Dealer Model."""

    __tablename__ = "crm_dealer"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_dealer_id = Column(Integer, unique=True)
    integration_partner_id = Column(Integer, ForeignKey("crm_integration_partner.id"))
    integration_partner = relationship(IntegrationPartner, backref=backref("dealers", lazy="dynamic"))

    crm_dealer_id = Column(Integer)
    sfdc_account_id = Column(String)
    dealer_name = Column(String)
    is_active = Column(Boolean, default=True)
    dealer_location_name = Column(String)
    country = Column(String)
    state = Column(String)
    city = Column(String)
    zip_code = Column(String)
    metadata_ = Column("metadata", JSONB)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
