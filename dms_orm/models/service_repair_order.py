"""Service Repair Order Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.vehicle import Vehicle
from dms_orm.models.integration_partner import IntegrationPartner


class ServiceRepairOrder(BaseForModels):
    """Service Repair Order Model."""

    __tablename__ = 'service_repair_order'

    id = Column(Integer, primary_key=True)
    partner_id = Column(Integer, ForeignKey('integration_partner.id'))
    consumer_id = Column(Integer, ForeignKey('consumer.id'))
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'))
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

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
