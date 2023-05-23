from ..base_model import BaseForModels
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from datetime import datetime
from sqlalchemy.orm import relationship


class ServiceContract(BaseForModels):
    """ServiceContract Model."""

    __tablename__ = "service_contracts"
    __table_args__ = { "schema": "stage" }

    id = Column(Integer, primary_key=True)
    # dealer_integration_partner_id = Column(Integer, ForeignKey("dealer_integration_partner.id"))
    consumer_id = Column(Integer, ForeignKey("stage.consumer.id"))
    vehicle_id = Column(Integer, ForeignKey("stage.vehicle.id"))
    # TODO not in DB
    sale_id = Column(Integer, ForeignKey("stage.vehicle_sale.id"))
    contract_id = Column(Integer)
    contract_name = Column(String)
    start_date = Column(DateTime)
    amount = Column(Float)
    cost = Column(Float)
    deductible = Column(Float)
    expiration_months = Column(String)
    expiration_miles = Column(Float)
    db_creation_date = Column(DateTime, default=datetime.utcnow())

    consumer = relationship('Consumer')
    vehicle = relationship('Vehicle')
    sale = relationship('VehicleSale')


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }