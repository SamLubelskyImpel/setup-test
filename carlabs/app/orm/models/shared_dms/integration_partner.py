"""Integration Partner Model."""

import sys

from ..base_model import BaseForModels
from sqlalchemy import Column, Integer, String


class IntegrationPartner(BaseForModels):
    """Integration Partner Model."""

    __tablename__ = "integration_partner"

    id = Column(Integer)
    name = Column(String, primary_key=True)
    type = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
