"""Lead_Salesperson Model."""

from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer, Boolean
from sqlalchemy.orm import backref, relationship
from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson


class Lead_Salesperson(BaseForModels):
    """Lead_Salesperson Model."""

    __tablename__ = "crm_lead_salesperson"

    id = Column(Integer, primary_key=True, autoincrement=True)

    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref=backref("leads", lazy="dynamic"))

    salesperson_id = Column(Integer, ForeignKey("crm_salesperson.id"))
    salesperson = relationship(Salesperson, backref=backref("salespersons", lazy="dynamic"))

    is_primary = Column(Boolean)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
