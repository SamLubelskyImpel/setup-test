import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String

class SFDCAccount(BaseForModels):
    __tablename__ = 'sfdc_account'

    id = Column(Integer)
    sfdc_account_id = Column(String, primary_key=True)
    customer_type = Column(String)
