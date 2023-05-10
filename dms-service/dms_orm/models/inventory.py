"""Inventory Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer
from dms_orm.models.dealer_group import DealerGroup
from dms_orm.models.vehicle import Vehicle


class Inventory(BaseForModels):
    """Inventory Model."""

    __tablename__ = 'inventory'

    id = Column(Integer, primary_key=True)
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'))
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    upload_date = Column(DateTime)
    list_price = Column(Float)
    msrp = Column(Float)
    invoice_price = Column(Float)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }