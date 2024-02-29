from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

class InvOptionInventory(BaseForModels):
    """Inv Option Inventory Model."""

    __tablename__ = "inv_option_inventory"

    id = Column(Integer, primary_key=True)
    inv_option_id = Column(Integer, ForeignKey("inv_option.id"), nullable=False)
    inv_inventory_id = Column(Integer, ForeignKey("inv_inventory.id"), nullable=False)

    # Define relationships
    option = relationship("InvOption")
    inventory = relationship("InvInventory")

    __table_args__ = (
        UniqueConstraint(
            "inv_option_id", "inv_inventory_id",
            name="unique_inv_option_inventory",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
