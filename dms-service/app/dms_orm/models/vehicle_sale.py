"""Vehicle Sale Model."""

import sys

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import BaseForModels
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import (
    text,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)


class VehicleSale(BaseForModels):
    """Vehicle Sale Model."""

    __tablename__ = "vehicle_sale"

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(
        Integer, ForeignKey("dealer_integration_partner.id")
    )
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    consumer_id = Column(Integer, ForeignKey("consumer.id"))
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
    trade_in_value = Column(Float)
    payoff_on_trade = Column(Float)
    value_at_end_of_lease = Column(Float)
    miles_per_year = Column(Integer)
    profit_on_sale = Column(Float)
    has_service_contract = Column(Boolean)
    vehicle_gross = Column(Float)
    delivery_date = Column(DateTime)
    db_creation_date = Column(DateTime)
    vin = Column(String)
    __table_args__ = (
        UniqueConstraint(
            "dealer_integration_partner_id",
            "sale_date",
            "vin",
            name="dealer_group_name_key",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
