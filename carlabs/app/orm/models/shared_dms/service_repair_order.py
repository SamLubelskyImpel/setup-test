'''Service Repair Order Model.'''

import sys

from ..base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, JSON
from datetime import datetime
from sqlalchemy.orm import relationship


class ServiceRepairOrder(BaseForModels):
    '''Service Repair Order Model.'''

    __tablename__ = 'service_repair_order'
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(Integer)
    consumer_id = Column(Integer, ForeignKey(f'{SCHEMA}.consumer.id'))
    vehicle_id = Column(Integer, ForeignKey(f'{SCHEMA}.vehicle.id'))
    ro_open_date = Column(DateTime)
    ro_close_date = Column(DateTime)
    txn_pay_type = Column(String)
    repair_order_no = Column(String)
    advisor_name = Column(String)
    total_amount = Column(Float)
    consumer_total_amount = Column(Float)
    warranty_total_amount = Column(Float)
    comment = Column(String)
    recommendation = Column(String)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    metadata_column = Column('metadata', JSON)

    consumer = relationship('Consumer')
    vehicle = relationship('Vehicle')


    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
