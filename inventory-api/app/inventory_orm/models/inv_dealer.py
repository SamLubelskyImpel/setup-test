from inventory_orm.session_config import BaseForModels
from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint

class InvDealer(BaseForModels):
    """Inv Dealer Model."""

    __tablename__ = "inv_dealer"

    id = Column(Integer, primary_key=True)
    impel_dealer_id = Column(String(100), nullable=False)
    location_name = Column(String(80), nullable=True)
    state = Column(String(20), nullable=True)
    city = Column(String(40), nullable=True)
    zip_code = Column(String(20), nullable=True)
    db_creation_date = Column(DateTime, nullable=False)
    db_update_date = Column(DateTime, nullable=True)
    db_update_user = Column(String(255), nullable=True)
    full_name = Column(String(255), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "impel_dealer_id",
            name="unique_impel_id",
        ),
    )

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
