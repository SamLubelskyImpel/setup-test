import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String
from universal_integrations_db.models.dealer_group import DealerGroup
from universal_integrations_db.models.sdfc_account import SDFCAccount


class Dealer(BaseForModels):
    __tablename__ = 'dealer'

    id = Column(Integer, primary_key=True)
    sdfc_account_id = Column(Integer, ForeignKey('sdfc_account.sdfc_account_id'))
    dealer_group_id = Column(Integer, ForeignKey('dealer_group.id'))
    name = Column(String)
    location_name = Column(String)
    state = Column(String)
    city = Column(String)
    zip_code = Column(String)
    category = Column(String)
