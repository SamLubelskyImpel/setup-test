"""Salesperson Model."""

from datetime import datetime
from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, DateTime, Integer, String


class Salesperson(BaseForModels):
    """Salesperson Model."""

    __tablename__ = "crm_salesperson"

    id = Column(Integer, primary_key=True)
    crm_salesperson_id = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    middle_name = Column(String)
    email = Column(String)
    phone = Column(String)
    db_creation_date = Column(DateTime, default=datetime.utcnow())
    db_update_date = Column(DateTime)
    db_update_role = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
