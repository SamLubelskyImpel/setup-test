"""Vehicle Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB


class Vehicle(BaseForModels):
    """Vehicle Model."""

    __tablename__ = "vehicle"

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(
        Integer, ForeignKey("dealer_integration_partner.id")
    )
    vin = Column(String)
    oem_name = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    new_or_used = Column(String)
    db_creation_date = Column(DateTime)
    stock_num = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
