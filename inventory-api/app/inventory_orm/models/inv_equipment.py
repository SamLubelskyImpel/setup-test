from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, Boolean, ForeignKey

class InvEquipment(BaseForModels):
    """Inv Equipment Model."""

    __tablename__ = "inv_equipment"

    id = Column(Integer, primary_key=True)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_user = Column(String(255), nullable=True)
    equipment_description = Column(String(255), nullable=True)
    is_optional = Column(Boolean, nullable=True)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
