"""Dealer Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, ForeignKey, String
from dms_orm.models.dealer_group import DealerGroup
from dms_orm.models.sfdc_account import SFDCAccount


class Dealer(BaseForModels):
    """Dealer Model."""

    __tablename__ = 'dealer'

    id = Column(Integer, primary_key=True)
    sfdc_account_id = Column(Integer, ForeignKey('sfdc_account.sdfc_account_id'))
    dealer_group_id = Column(Integer, ForeignKey('dealer_group.id'))
    name = Column(String)
    location_name = Column(String)
    state = Column(String)
    city = Column(String)
    zip_code = Column(String)
    category = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }