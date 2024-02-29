from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, Boolean, ForeignKey

class InvOption(BaseForModels):
    """Inv Option Model."""

    __tablename__ = "inv_option"

    id = Column(Integer, primary_key=True)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_user = Column(String(255), nullable=True)
    option_description = Column(String(255), nullable=True)
    is_priority = Column(Boolean, nullable=True)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
