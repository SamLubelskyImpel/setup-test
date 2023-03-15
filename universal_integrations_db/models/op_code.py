import sys

from session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer, String
from models.dealer import Dealer

class OpCode(BaseForModels):
    __tablename__ = 'op_code'

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    op_code = Column(String)
    op_code_desc = Column(String)
