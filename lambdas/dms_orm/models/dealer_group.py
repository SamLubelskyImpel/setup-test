"""DealerGroup Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class DealerGroup(BaseForModels):
    """DealerGroup Model."""

    __tablename__ = 'dealer_group'

    id = Column(Integer)
    name = Column(String, primary_key=True)
    duns_no = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }