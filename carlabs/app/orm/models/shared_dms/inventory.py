"""Inventory Model."""

import sys

from ..base_model import BaseForModels
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer


class Inventory(BaseForModels):
    """Inventory Model."""

    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True)
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    upload_date = Column(DateTime)
    list_price = Column(Float)
    msrp = Column(Float)
    invoice_price = Column(Float)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
