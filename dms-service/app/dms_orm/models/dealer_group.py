"""DealerGroup Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint


class DealerGroup(BaseForModels):
    """DealerGroup Model."""

    __tablename__ = "dealer_group"

    id = Column(Integer, primary_key=True)
    impel_dealer_group_id = Column(String)
    duns_no = Column(String)
    db_creation_date = Column(DateTime)
    __table_args__ = (UniqueConstraint("impel_dealer_group_id", name="dealer_group_name_key"),)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
