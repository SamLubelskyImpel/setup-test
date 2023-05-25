"""Service Repair Order Model."""

import sys

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import BaseForModels
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)


class ServiceRepairOrder(BaseForModels):
    """Service Repair Order Model."""

    __tablename__ = "service_repair_order"

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(
        Integer, ForeignKey("dealer_integration_partner.id")
    )
    consumer_id = Column(Integer, ForeignKey("consumer.id"))
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"))
    ro_open_date = Column(DateTime)
    ro_close_date = Column(DateTime)
    txn_pay_type = Column(String)
    repair_order_no = Column(String)
    advisor_name = Column(String)
    total_amount = Column(Float)
    consumer_total_amount = Column(Float)
    warranty_total_amount = Column(Float)
    comment = Column(String)
    recommendation = Column(String)
    db_creation_date = Column(DateTime)
    __table_args__ = (
        UniqueConstraint(
            "repair_order_no", "dealer_integration_partner_id", name="unique_ros_dms"
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
