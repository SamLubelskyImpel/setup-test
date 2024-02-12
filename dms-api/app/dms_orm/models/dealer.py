"""Dealer Model."""

import sys

from dms_orm.models.dealer_group import DealerGroup
from dms_orm.models.sfdc_account import SFDCAccount
from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint


class Dealer(BaseForModels):
    """Dealer Model."""

    __tablename__ = "dealer"

    id = Column(Integer, primary_key=True)
    dealer_group_id = Column(Integer, ForeignKey("dealer_group.id"))
    impel_dealer_id = Column(String)
    location_name = Column(String)
    state = Column(String)
    city = Column(String)
    zip_code = Column(String)
    db_creation_date = Column(DateTime)
    __table_args__ = (
        UniqueConstraint("impel_dealer_id", name="unique_impel_dealer_id"),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
