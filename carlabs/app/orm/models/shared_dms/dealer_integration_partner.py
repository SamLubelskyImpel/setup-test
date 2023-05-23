"""Consumer Model."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from ..base_model import BaseForModels
from datetime import datetime


class DealerIntegrationPartner(BaseForModels):
    """Consumer Model."""

    __tablename__ = "dealer_integration_partner"
    # TODO set schema according to environment
    __table_args__ = { "schema": "stage" }

    id = Column(Integer, primary_key=True)
    sfdc_account_id = Column(Integer, ForeignKey("sfdc_account.sdfc_account_id"))
    interation_id = Column(Integer, ForeignKey("integration_partner.id"))
    dealer_integration_partner_id = Column(Integer, ForeignKey("dealer_integration_partner.id"))
    dealer_customer_no = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String)
    ip_address = Column(String)
    cell_phone = Column(String)
    city = Column(String)
    state = Column(String)
    metro = Column(String)
    postal_code = Column(String)
    home_phone = Column(String)
    email_optin_flag = Column(Boolean)
    phone_optin_flag = Column(Boolean)
    postal_mail_optin_flag = Column(Boolean)
    sms_optin_flag = Column(Boolean)
    # TODO not in the DB
    address = Column(String)
    db_creation_date = Column(DateTime)


    def as_dict(self):
        """Return attributes of the keys in the table."""
        return {
            key.name: getattr(self, key.name)
            for key in self.__table__.columns
            if getattr(self, key.name) is not None
        }
