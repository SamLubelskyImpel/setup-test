"""Vehicle Sale Model."""

import sys

from ..base_model import BaseForModels
from sqlalchemy import (JSON, Boolean, Column, DateTime, Float, ForeignKey,
                        Integer, String)


class VehicleSale(BaseForModels):
    """Vehicle Sale Model."""

    __tablename__ = "vehicle_sale"

    id = Column(Integer, primary_key=True)
    consumer_id = Column(Integer, ForeignKey("consumer.id"))
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    sale_date = Column(DateTime)
    listed_price = Column(Float)
    sales_tax = Column(Float)
    mileage_on_vehicle = Column(Integer)
    deal_type = Column(String)
    cost_of_vehicle = Column(Float)
    oem_msrp = Column(Float)
    adjustment_on_price = Column(Float)
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
    service_package_flag = Column(Boolean)
    db_creation_date = Column(DateTime)
    vin = Column(String)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    delivery_date = Column(DateTime)
    finance_rate = Column(Float)
    finance_term = Column(Float)
    finance_amount = Column(Float)
    date_of_inventory = Column(DateTime)
    si_load_process = Column(String)
    si_load_timestamp = Column(DateTime)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
