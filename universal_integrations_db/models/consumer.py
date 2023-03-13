import sys

from session_config import BaseForModels
from sqlalchemy import Boolean, Column, Integer, String
from universal_integrations_db.models.sdfc_account import SDFCAccount
from universal_integrations_db.models.dealer_group import DealerGroup


class Consumer(BaseForModels):
    __tablename__ = 'consumer'

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
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
    mastter_consumer_id = Column(Integer)
