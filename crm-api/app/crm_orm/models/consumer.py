"""Consumer Model."""

from sqlalchemy.orm import backref, relationship
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import BaseForModels
from sqlalchemy import Boolean, Column, ForeignKey, DateTime, Integer, String


class Consumer(BaseForModels):
    """Consumer Model."""

    __tablename__ = "crm_consumer"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_consumer_id = Column(String)
    dealer_id = Column(Integer, ForeignKey("crm_dealer.id"))
    dealer = relationship(Dealer, backref=backref("consumers", lazy="dynamic"))

    first_name = Column(String)
    last_name = Column(String)
    middle_name = Column(String)
    email = Column(String)
    phone = Column(String)
    address = Column(String)
    country = Column(String)
    city = Column(String)
    request_product = Column(String)
    postal_code = Column(String)
    email_optin_flag = Column(Boolean)
    sms_optin_flag = Column(Boolean)
    db_creation_date = Column(DateTime)
    db_update_date = Column(DateTime)
    db_update_role = Column(String)

    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
        }
