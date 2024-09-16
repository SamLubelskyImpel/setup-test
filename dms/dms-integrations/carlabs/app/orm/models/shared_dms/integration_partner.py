"""Integration Partner Model."""
from ..base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, Integer, String, DateTime


class IntegrationPartner(BaseForModels):
    """Integration Partner Model."""

    __tablename__ = "integration_partner"
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    impel_integration_partner_id = Column(String)
    type = Column(String)
    db_creation_date = Column(DateTime)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
