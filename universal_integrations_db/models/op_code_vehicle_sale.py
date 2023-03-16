"""OP Code Vehicle Sale Model."""

from session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer
from models.op_code import OpCode
from models.vehicle_sale import VehicleSale


class OpCodeVehicleSale(BaseForModels):
    """OP Code Vehicle Sale Model."""

    __tablename__ = 'op_code_vehicle_sale'

    id = Column(Integer, primary_key=True)
    op_code_id = Column(Integer, ForeignKey('op_code.id'))
    vehicle_sale_id = Column(Integer, ForeignKey('vehicle_sale.id'))

