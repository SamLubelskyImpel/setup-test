"""Vehicle Model."""

from datetime import datetime
from sqlalchemy.orm import backref, relationship
from crm_orm.models.dealer import Dealer
from crm_orm.models.lead import Lead
from crm_orm.session_config import BaseForModels
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Numeric
from sqlalchemy.dialects.postgresql import JSONB


class Vehicle(BaseForModels):
    """Vehicle Model."""

    __tablename__ = "crm_vehicle"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(Integer, ForeignKey("crm_lead.id"))
    lead = relationship(Lead, backref=backref("vehicles", lazy="dynamic"))

    vin = Column(String, unique=True)
    crm_vehicle_id = Column(String)
    type = Column(String)
    vehicle_class = Column(String)
    mileage = Column(Integer)
    make = Column(String)
    model = Column(String)
    manufactured_year = Column(Integer)
    body_style = Column(String)
    transmission = Column(String)
    interior_color = Column(String)
    exterior_color = Column(String)
    trim = Column(String)
    price = Column(Numeric)
    status = Column(String)
    condition = Column(String)
    odometer_units = Column(String)
    vehicle_comments = Column(String)
    metadata_ = Column("metadata", JSONB)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)
    db_update_role = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
