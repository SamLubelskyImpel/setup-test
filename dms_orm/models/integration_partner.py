"""Integration Partner Model."""

import sys

sys.path.append("..") 
from session_config import BaseForModels
from sqlalchemy import Column, Integer, String


class IntegrationPartner(BaseForModels):
    """Integration Partner Model."""

    __tablename__ = 'integration_partner'

    id = Column(Integer)
    name = Column(String, primary_key=True)
    type = Column(String)
