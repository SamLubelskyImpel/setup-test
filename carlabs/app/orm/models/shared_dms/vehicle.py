"""Vehicle Model."""

import sys

from ..base_model import BaseForModels
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey


class Vehicle(BaseForModels):
    """Vehicle Model."""

    __tablename__ = "vehicle"

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    vin = Column(String)
    oem_name = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    db_creation_date = Column(DateTime)
    si_load_process = Column(String)
    si_load_timestamp = Column(DateTime)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
