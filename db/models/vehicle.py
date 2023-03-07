import sys

# TODO: find cleaner way for this later
sys.path.append('../')

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class Vehicle(BaseForModels):
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

