"""Vehicle Sale Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, JSON, String
from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.vehicle import Vehicle


class VehicleSale(BaseForModels):
    """Vehicle Sale Model."""

    __tablename__ = 'vehicle_sale'

    id = Column(Integer, primary_key=True)
    consumer_id = Column(Integer, ForeignKey('consumer.id'))
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'))
    sale_date = Column(DateTime)
    listed_price = Column(Float)
    sales_tax = Column(Float)
    mileage_on_vehicle = Column(Integer)
    deal_type = Column(String)
    cost_of_vehicle = Column(Float)
    oem_msrp = Column(Float)
    discount_on_price = Column(Float)
    days_in_stock = Column(Integer)
    date_of_state_inspection = Column(DateTime)
    is_new = Column(Boolean)
    trade_in_value = Column(Float)
    payoff_on_trade = Column(Float)
    value_at_end_of_lease = Column(Float)
    miles_per_year = Column(Integer)
    profit_on_sale = Column(Float)
    has_service_contract = Column(Boolean)
    vehicle_gross = Column(Float)
    warranty_expiration_date = Column(DateTime)
    service_package = Column(JSON)
    extended_warranty = Column(JSON)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }