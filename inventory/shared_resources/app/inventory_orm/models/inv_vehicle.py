from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.ext.mutable import MutableDict
from datetime import datetime


class InvVehicle(BaseForModels):
    """Inv Vehicle Model."""

    __tablename__ = "inv_vehicle"

    id = Column(Integer, primary_key=True)
    vin = Column(String, nullable=True)
    oem_name = Column(String(80), nullable=True)
    type = Column(String(255), nullable=True)
    vehicle_class = Column(String(255), nullable=True)
    mileage = Column(String(255), nullable=True)
    make = Column(String(80), nullable=True)
    model = Column(String(80), nullable=True)
    year = Column(Integer, nullable=True)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_role = Column(String(255), nullable=True)
    dealer_integration_partner_id = Column(Integer, ForeignKey("inv_dealer_integration_partner.id"), nullable=False)
    new_or_used = Column(String(1), nullable=True)
    # metadata_ = Column("metadata", MutableDict.as_mutable(JSONB)) Removed because currently metadata values are string and not json
    stock_num = Column(String(255), nullable=True)

    # Define relationship
    dealer_integration_partner = relationship("InvDealerIntegrationPartner")