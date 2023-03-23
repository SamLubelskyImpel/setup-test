"""Vehicle Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class Vehicle(BaseForModels):
    """Vehicle Model."""

    __tablename__ = 'vehicle'

    id = Column(Integer, primary_key=True)
    vin = Column(String)
    oem_name = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }