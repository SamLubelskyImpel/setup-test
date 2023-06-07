"""SFDC Account Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint


class SFDCAccount(BaseForModels):
    """SFDC Account Model."""

    __tablename__ = "sfdc_account"

    id = Column(Integer, primary_key=True)
    sfdc_account_id = Column(String)
    customer_type = Column(String)
    db_creation_date = Column(DateTime)
    __table_args__ = (
        UniqueConstraint("sfdc_account_id", name="sfdc_account_sfdc_account_id_key"),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
