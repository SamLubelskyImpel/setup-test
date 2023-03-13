import sys

from session_config import BaseForModels
from sqlalchemy import Boolean, Column, DateTime, Float Integer, JSON, String
from universal_integrations_db.models.consumer import Consumer
from universal_integrations_db.models.dealer import Dealer
from universal_integrations_db.models.vehicle import Vehicle


class VehicleSale(BaseForModels):
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
    cost_of_vehicles = Column(Float)
    oem_msrp = Column(Float)
    discount_on_price = Column(Float)
    days_in_stock = Column(Integer)
    date_of_state_inspection = Column(DateTime)
    is_new = Column(Boolean)
    trade_in_values = Column(Float)
    payoff_on_trade = Column(Float)
    value_at_end_of_lease = Column(Float)
    miles_per_year = Column(Integer)
    profit_on_sale = Column(Float)
    has_service_contract = Column(Boolean)
    vehicle_gross = Column(Float)
    warranty_expiration_date = Column(DateTime)
    service_package = Column(JSON)
    extended_warranty = Column(JSON)
