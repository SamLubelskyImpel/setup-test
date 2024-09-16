"""Integration Partner Model."""

from sqlalchemy import Column, DateTime, Integer, String
from typing import Dict, Any
from crm_orm.session_config import BaseForModels


class IntegrationPartner(BaseForModels):  # type: ignore
    """Integration Partner Model."""

    __tablename__ = "crm_integration_partner"

    id = Column(Integer, primary_key=True, autoincrement=True)
    impel_integration_partner_name = Column(String)
    integration_date = Column(DateTime)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)

    def as_dict(self) -> Dict[str, Any]:
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
