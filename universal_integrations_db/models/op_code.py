import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String

class OpCode(BaseForModels):
    __tablename__ = 'op_code'

    id = Column(Integer)
    sdfc_account_id = Column(String, primary_key=True)
    customer_type = Column(String)
