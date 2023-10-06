"""Activity Type Model."""

from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String, DateTime


class ActivityType(BaseForModels):
    """Activity Type Model."""

    __tablename__ = "crm_activity_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)
    db_update_role = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
