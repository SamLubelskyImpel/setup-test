from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Boolean, Integer, String, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.schema import MetaData
from datetime import datetime
import json


class InvDealerIntegrationPartner(BaseForModels):
    """Inv Dealer Integration Partner Model."""

    __tablename__ = "inv_dealer_integration_partner"

    id = Column(Integer, primary_key=True)
    integration_partner_id = Column(Integer, ForeignKey("inv_integration_partner.id"), nullable=False)
    dealer_id = Column(Integer, ForeignKey("inv_dealer.id"), nullable=False)
    provider_dealer_id = Column(String(255), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_role = Column(String(255), nullable=True)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))

    # Define relationships
    integration_partner = relationship("InvIntegrationPartner")
    dealer = relationship("InvDealer")

    __table_args__ = (
        UniqueConstraint(
            "dealer_id", "integration_partner_id", "provider_dealer_id",
            name="dealer_integration_partner_un",
        ),
    )