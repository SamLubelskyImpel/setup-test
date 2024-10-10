"""Lead_Salesperson Model."""

from sqlalchemy import Column, ForeignKey, Integer, Boolean, DateTime
from sqlalchemy.orm import relationship
from typing import Dict, Any
from crm_orm.session_config import BaseForModels
from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson


class Lead_Salesperson(BaseForModels):  # type: ignore
    """Lead_Salesperson Model."""

    __tablename__ = "crm_lead_salesperson"

    id = Column(Integer, primary_key=True, autoincrement=True)

    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref="lead_salespersons")

    salesperson_id = Column(Integer, ForeignKey("crm_salesperson.id"))
    salesperson = relationship(Salesperson, backref="lead_salespersons")

    is_primary = Column(Boolean)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
