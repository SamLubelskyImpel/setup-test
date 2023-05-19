from dataclasses import dataclass
from .base import BaseMapping
from typing import Optional


@dataclass
class ServiceContractTableMapping(BaseMapping):

    contract_name: str
    start_date: str
    amount: str
    cost: str
    deductible: str
    expiration_months: str
    expiration_miles: str
    dealer_id: Optional[str] = 'dealerCode'
    db_creation_date: Optional[str] = 'creationDate'