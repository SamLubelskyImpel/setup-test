from ..base_model import BaseForModels
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float


class ServiceContract(BaseForModels):
    """ServiceContract Model."""

    __tablename__ = "service_contract"

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    consumer_id = Column(Integer, ForeignKey("consumer.id"))
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    sale_id = Column(Integer, ForeignKey("vehicle_sale.id"))
    contract_id = Column(Integer)
    contract_name = Column(String)
    start_date = Column(DateTime)
    amount = Column(Float)
    cost = Column(Float)
    deductible = Column(Float)
    expiration_months = Column(String)
    expiration_miles = Column(Float)
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