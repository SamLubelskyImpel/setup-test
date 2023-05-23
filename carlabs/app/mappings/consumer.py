from dataclasses import dataclass
from .base import BaseMapping
from typing import Optional


@dataclass
class ConsumerTableMapping(BaseMapping):

    dealer_customer_no: str
    first_name: str
    last_name: str
    email: str
    ip_address: str
    cell_phone: str
    city: str
    state: str
    metro: str
    postal_code: str
    home_phone: str
    email_optin_flag: str
    phone_optin_flag: str
    postal_mail_optin_flag: str
    sms_optin_flag: str
    master_consumer_id: str
    address: str
    # dealer_integration_partner_id: Optional[str] = 'dealerCode'