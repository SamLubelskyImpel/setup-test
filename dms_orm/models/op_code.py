"""OP Code Model."""

import sys

from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, ForeignKey, Integer, String
from dms_orm.models.dealer import Dealer


class OpCode(BaseForModels):
    """OP Code Model."""

    __tablename__ = 'op_code'

    id = Column(Integer, primary_key=True)
    dealer_id = Column(Integer, ForeignKey('dealer.id'))
    op_code = Column(String)
    op_code_desc = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name) for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }