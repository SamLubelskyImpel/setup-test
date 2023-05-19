"""Dealer Model."""

import sys

from dms_orm.models.dealer_group import DealerGroup
from dms_orm.models.sfdc_account import SFDCAccount
from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint


class Dealer(BaseForModels):
    """Dealer Model."""

    __tablename__ = "dealer"

    id = Column(Integer, primary_key=True)
    dealer_group_id = Column(Integer, ForeignKey("dealer_group.id"))
    impel_id = Column(String)
    location_name = Column(String)
    state = Column(String)
    city = Column(String)
    zip_code = Column(String)
    __table_args__ = (UniqueConstraint("impel_id", name="unique_impel_id"),)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
