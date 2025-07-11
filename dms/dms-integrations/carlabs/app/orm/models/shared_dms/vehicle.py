'''Vehicle Model.'''

import sys

from ..base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime


class Vehicle(BaseForModels):
    '''Vehicle Model.'''

    __tablename__ = 'vehicle'
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(Integer)
    vin = Column(String)
    oem_name = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    new_or_used = Column(String)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    metadata_column = Column('metadata', JSON)

    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
