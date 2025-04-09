from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.ext.mutable import MutableDict

class InvInventory(BaseForModels):
    """Inv Inventory Model."""

    __tablename__ = "inv_inventory"

    id = Column(Integer, primary_key=True)
    vehicle_id = Column(Integer, ForeignKey("inv_vehicle.id"), nullable=False)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_role = Column(String(255), nullable=True)
    list_price = Column(Float, nullable=True)
    cost_price = Column(Float, nullable=True)
    fuel_type = Column(String(255), nullable=True)
    exterior_color = Column(String(255), nullable=True)
    interior_color = Column(String(255), nullable=True)
    doors = Column(Integer, nullable=True)
    seats = Column(Integer, nullable=True)
    transmission = Column(String(255), nullable=True)
    photo_url = Column(String(255), nullable=True)
    comments = Column(String(255), nullable=True)
    drive_train = Column(String(255), nullable=True)
    cylinders = Column(Integer, nullable=True)
    body_style = Column(String(255), nullable=True)
    series = Column(String(255), nullable=True)
    on_lot = Column(Boolean, nullable=True)
    inventory_status = Column(String(255), nullable=True)
    vin = Column(String(255), nullable=True)
    dealer_integration_partner_id = Column(Integer, ForeignKey("inv_dealer_integration_partner.id"), nullable=False)
    interior_material = Column(String(255), nullable=True)
    source_data_drive_train = Column(String(255), nullable=True)
    source_data_interior_material_description = Column(String(255), nullable=True)
    source_data_transmission = Column(String(255), nullable=True)
    source_data_transmission_speed = Column(String(255), nullable=True)
    transmission_speed = Column(String(255), nullable=True)
    build_data = Column(String(255), nullable=True)
    region = Column(String(255), nullable=True)
    highway_mpg = Column(Integer, nullable=True)
    city_mpg = Column(Integer, nullable=True)
    vdp = Column(String(255), nullable=True)
    trim = Column(String(255), nullable=True)
    special_price = Column(Float, nullable=True)
    engine = Column(String(255), nullable=True)
    engine_displacement = Column(String(255), nullable=True)
    factory_certified = Column(Boolean, nullable=True)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))
    options = Column(ARRAY(String(255)), nullable=True)
    priority_options = Column(ARRAY(String(255)), nullable=True)

    # Define relationship
    vehicle = relationship("InvVehicle")

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }

    def as_dict_custom(self):
        """Return attributes of the keys and rename id field."""
        result = {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }

        result["inv_inventory_id"] = result.pop("id")

        return result