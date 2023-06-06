"""Inventory Model."""

import sys

from dms_orm.models.dealer_group import DealerGroup
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, UniqueConstraint


class Inventory(BaseForModels):
    """Inventory Model."""

    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True)
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    dealer_integration_partner_id = Column(
        Integer, ForeignKey("dealer_integration_partner.id")
    )
    upload_date = Column(DateTime)
    list_price = Column(Float)
    msrp = Column(Float)
    invoice_price = Column(Float)
    db_creation_date = Column(DateTime)
    __table_args__ = (
        UniqueConstraint(
            "vehicle_id", "dealer_integration_partner_id", name="unique_inventory"
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
