"""Dealer Integration Partner Model."""
from .base_model import BaseForModels, SCHEMA
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship


class DealerIntegrationPartner(BaseForModels):
    """Dealer Integration Partner Model."""

    __tablename__ = "dealer_integration_partner"
    __table_args__ = {'schema': SCHEMA}

    id = Column(Integer, primary_key=True)
    integration_partner_id = Column(Integer)
    dealer_id = Column(Integer, ForeignKey(f'{SCHEMA}.dealer.id'))
    dms_id = Column(String)
    is_active = Column(Boolean)
    db_creation_date = Column(DateTime)

    dealer = relationship('Dealer')


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
