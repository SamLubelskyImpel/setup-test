import sys

from session_config import BaseForModels
from sqlalchemy import Column, Integer
from universal_integrations_db.models.op_code import OpCode
from universal_integrations_db.models.vehicle_sale import VehicleSale


class OpCodeVehicleSale(BaseForModels):
    __tablename__ = 'op_code_vehicle_sale'

    id = Column(Integer)
    op_code_id = Column(Integer, ForeignKey('op_code.id'))
    vehicle_sale_id = Column(Integer, ForeignKey('vehicle_sale.id'))

