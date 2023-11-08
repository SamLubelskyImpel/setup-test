"""Appointment Model."""

import sys

from dms_orm.models.dealer_group import DealerGroup
from dms_orm.session_config import BaseForModels
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, text, DateTime, Time, Date
from sqlalchemy.dialects.postgresql import JSONB


class Appointment(BaseForModels):
    """Appointment Model."""

    __tablename__ = "appointment"

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(Integer, ForeignKey("dealer_integration_partner.id"))
    consumer_id = Column(Integer, ForeignKey("consumer.id"))
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    appointment_time = Column(Time)
    appointment_date = Column(Date)
    appointment_source = Column(String)
    reason_code = Column(String)
    appointment_create_ts = Column(DateTime)
    appointment_update_ts = Column(DateTime)
    rescheduled_flag = Column(Boolean)
    appointment_no = Column(Integer)
    last_ro_date = Column(Date)
    last_ro_num = Column(Integer)
    db_creation_date = Column(DateTime)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
