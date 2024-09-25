from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

class InvEquipmentInventory(BaseForModels):
    """Inv Equipment Inventory Model."""

    __tablename__ = "inv_equipment_inventory"

    id = Column(Integer, primary_key=True)
    inv_equipment_id = Column(Integer, ForeignKey("inv_equipment.id"), nullable=False)
    inv_inventory_id = Column(Integer, ForeignKey("inv_inventory.id"), nullable=False)

    # Define relationships
    equipment = relationship("InvEquipment")
    inventory = relationship("InvInventory")

    __table_args__ = (
        UniqueConstraint(
            "inv_equipment_id", "inv_inventory_id",
            name="unique_inv_equipment_inventory",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
