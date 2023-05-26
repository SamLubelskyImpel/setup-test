"""OP Code Model."""

import sys

from dms_orm.models.dealer import Dealer
from dms_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint


class OpCode(BaseForModels):
    """OP Code Model."""

    __tablename__ = "op_code"

    id = Column(Integer, primary_key=True)
    dealer_integration_partner_id = Column(
        Integer, ForeignKey("dealer_integration_partner.id")
    )
    op_code = Column(String)
    op_code_desc = Column(String)
    db_creation_date = Column(DateTime)
    __table_args__ = (
        UniqueConstraint(
            "dealer_integration_partner_id",
            "op_code",
            "op_code_desc",
            name="unique_op_code",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
