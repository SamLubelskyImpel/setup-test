"""Vehicle Sale Model."""

import sys

from ..base_model import BaseForModels, DoublePrecisionField
from sqlalchemy import (JSON, Boolean, Column, DateTime, Float, ForeignKey,
                        Integer, String)
from datetime import datetime
from sqlalchemy.orm import relationship


class VehicleSale(BaseForModels):
    """Vehicle Sale Model."""

    __tablename__ = "vehicle_sale"
    __table_args__ = { "schema": "stage" }

    id = Column(Integer, primary_key=True)
    consumer_id = Column(Integer, ForeignKey("stage.consumer.id"))
    # dealer_integration_partner_id = Column(Integer, ForeignKey("dealer_integration_partner.id"))
    vehicle_id = Column(Integer, ForeignKey("stage.vehicle.id"))
    sale_date = Column(DateTime)
    listed_price = Column(DoublePrecisionField)
    sales_tax = Column(DoublePrecisionField)
    mileage_on_vehicle = Column(Integer)
    deal_type = Column(String)
    cost_of_vehicle = Column(DoublePrecisionField)
    oem_msrp = Column(DoublePrecisionField)
    adjustment_on_price = Column(DoublePrecisionField)
    days_in_stock = Column(Integer)
    date_of_state_inspection = Column(DateTime)
    trade_in_value = Column(DoublePrecisionField)
    payoff_on_trade = Column(DoublePrecisionField)
    value_at_end_of_lease = Column(DoublePrecisionField)
    miles_per_year = Column(Integer)
    profit_on_sale = Column(DoublePrecisionField)
    has_service_contract = Column(Boolean)
    vehicle_gross = Column(DoublePrecisionField)
    warranty_expiration_date = Column(DateTime)
    # TODO column changed during mapping
    service_package_flag = Column(Boolean)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    delivery_date = Column(DateTime)
    # TODO all below are missing in the DB
    finance_rate = Column(DoublePrecisionField)
    finance_term = Column(DoublePrecisionField)
    finance_amount = Column(DoublePrecisionField)
    date_of_inventory = Column(DateTime)

    consumer = relationship('Consumer')
    vehicle = relationship('Vehicle')


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
