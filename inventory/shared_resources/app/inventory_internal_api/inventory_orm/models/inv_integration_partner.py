from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict

class InvIntegrationPartner(BaseForModels):
    """Inv Integration Partner Model."""

    __tablename__ = "inv_integration_partner"

    id = Column(Integer, primary_key=True)
    impel_integration_partner_id = Column(String(40), nullable=False)
    type = Column(String(20), nullable=True)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime, nullable=True)
    db_update_role = Column(String(255), nullable=True)
    metadata_ = Column("metadata", MutableDict.as_mutable(JSONB))

    __table_args__ = (
        UniqueConstraint(
            "impel_integration_partner_id",
            name="unique_inv_integration_partner_impel_id",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
