from .base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, JSON, Boolean
from datetime import datetime
from sqlalchemy.orm import relationship


class ServiceContract(BaseForModels):
    '''ServiceContract Model.'''

    __tablename__ = 'service_contracts'
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(Integer)
    consumer_id = Column(Integer, ForeignKey(f'{SCHEMA}.consumer.id'))
    vehicle_id = Column(Integer, ForeignKey(f'{SCHEMA}.vehicle.id'))
    contract_id = Column(Integer)
    contract_name = Column(String)
    start_date = Column(DateTime)
    amount = Column(Float)
    cost = Column(Float)
    deductible = Column(Float)
    expiration_months = Column(String)
    expiration_miles = Column(Float)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    warranty_expiration_date = Column(DateTime)
    extended_warranty = Column(JSON)
    service_package_flag = Column(Boolean)

    consumer = relationship('Consumer')
    vehicle = relationship('Vehicle')

    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
