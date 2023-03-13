import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class DealerGroup(BaseForModels):
    __tablename__ = 'dealer_group'

    id = Column(Integer)
    name = Column(String, primary_key=True)
    duns_no = Column(String)
