import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String

class SDFCAccount(BaseForModels):
    __tablename__ = 'sdfc_account'

    id = Column(Integer)
    sdfc_account_id = Column(String, primary_key=True)
    customer_type = Column(String)
