import sys

from session_config import BaseForModels
from sqlalchemy import Column, DateTime, Float, Integer
from universal_integrations_db.models.dealer_group import DealerGroup
from universal_integrations_db.models.sdfc_account import SDFCAccount
from universal_integrations_db.models.vehicle import Vehicle


class Inventory(BaseForModels):
    __tablename__ = 'inventory'

    id = Column(Integer, primary_key=True)
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'))
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    upload_date = Column(DateTime)
    list_price = Column(Float)
    msrp = Column(Float)
    invoice_price = Column(Float)
