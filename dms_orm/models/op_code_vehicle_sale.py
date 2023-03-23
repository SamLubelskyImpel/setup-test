"""OP Code Vehicle Sale Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer
from dms_orm.models.op_code import OpCode
from dms_orm.models.vehicle_sale import VehicleSale


class OpCodeVehicleSale(BaseForModels):
    """OP Code Vehicle Sale Model."""

    __tablename__ = 'op_code_vehicle_sale'

    id = Column(Integer, primary_key=True)
    op_code_id = Column(Integer, ForeignKey('op_code.id'))
    vehicle_sale_id = Column(Integer, ForeignKey('vehicle_sale.id'))

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }