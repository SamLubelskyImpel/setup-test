import sys

from session_config import BaseForModels
from sqlalchemy import Column, DateTime, Float Integer, String
from universal_integrations_db.models.consumer import Consumer
from universal_integrations_db.models.dealer import Dealer
from universal_integrations_db.models.vehicle import Vehicle
from universal_integrations_db.models.integration_partner import IntegrationPartner


class ServiceRepairOrder(BaseForModels):
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

