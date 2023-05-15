"""Consumer Model."""

import sys

from dms_orm.models.dealer_group import DealerGroup
from dms_orm.session_config import BaseForModels
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String


class Consumer(BaseForModels):
    """Consumer Model."""

    __tablename__ = "consumer"

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey("dealer.id"))
    dealer_customer_no = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String)
    ip_address = Column(String)
    cell_phone = Column(String)
    city = Column(String)
    state = Column(String)
    metro = Column(String)
    postal_code = Column(String)
    home_phone = Column(String)
    email_optin_flag = Column(Boolean)
    phone_optin_flag = Column(Boolean)
    postal_mail_optin_flag = Column(Boolean)
    sms_optin_flag = Column(Boolean)
    master_consumer_id = Column(Integer)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
