"""OP Code Model."""

import sys

from dms_orm.models.dealer import Dealer
from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint


class OpCodeRepairOrder(BaseForModels):
    """OP Code Repair Order Model."""

    __tablename__ = "op_code_repair_order"

    id = Column(Integer, primary_key=True)
    op_code_id = Column(Integer, ForeignKey("op_code.id"))
    repair_order_id = Column(Integer, ForeignKey("repair_order.id"))

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
