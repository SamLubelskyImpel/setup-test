from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text
from ..base_model import BaseForModels


class RepairOrder(BaseForModels):
    __tablename__ = 'repair_order'

    id = Column(Integer, primary_key=True)
    ro_source = Column(String)
    dealer_id = Column(String)
    ro_number = Column(String)
    ro_open_date = Column(DateTime)
    ro_close_date = Column(DateTime)
    total_amount = Column(Float)
    vin = Column(String)
    email_address = Column(String)
    warranty_flag = Column(Boolean)
    ro_service_details = Column(Text)
    dataimport_creation_date = Column(DateTime)
