"""SFDC Account Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class SFDCAccount(BaseForModels):
    """SFDC Account Model."""

    __tablename__ = 'sfdc_account'

    id = Column(Integer)
    sfdc_account_id = Column(String, primary_key=True)
    customer_type = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }