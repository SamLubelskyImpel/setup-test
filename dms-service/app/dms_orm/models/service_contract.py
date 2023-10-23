from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, JSON, Boolean
from datetime import datetime
from sqlalchemy.orm import relationship
from dms_orm.session_config import BaseForModels


class ServiceContract(BaseForModels):
    '''ServiceContract Model.'''

    __tablename__ = 'service_contracts'

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(Integer, ForeignKey("dealer_integration_partner.id"))
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
    vehicle_sale_id = Column(Integer, ForeignKey("Vehicle.id"))
    appointment_id = Column(Integer, ForeignKey("Appointment.id"))

    def as_dict(self):
        '''Return attributes of the keys in the table.'''
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
