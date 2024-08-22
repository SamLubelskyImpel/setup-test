"""Vehicle Model."""

from sqlalchemy import Column, Integer, String, ForeignKey, Numeric, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from typing import Dict, Any
from crm_orm.session_config import BaseForModels
from crm_orm.models.lead import Lead


class Vehicle(BaseForModels):   # type: ignore
    """Vehicle Model."""

    __tablename__ = "crm_vehicle"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref="vehicles")

    vin = Column(String, unique=True)
    stock_num = Column(String)
    crm_vehicle_id = Column(String)
    oem_name = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    manufactured_year = Column(Integer)
    body_style = Column(String)
    transmission = Column(String)
    interior_color = Column(String)
    exterior_color = Column(String)
    trim = Column(String)
    price = Column(Numeric)
    status = Column(String)
    condition = Column(String)
    odometer_units = Column(String)
    vehicle_comments = Column(String)
    trade_in_vin = Column(String)
    trade_in_year = Column(Integer)
    trade_in_make = Column(String)
    trade_in_model = Column(String)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))  # type: ignore
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
