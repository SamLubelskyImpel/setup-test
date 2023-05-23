from dataclasses import dataclass
from .base import BaseMapping
from typing import Optional


@dataclass
class VehicleTableMapping(BaseMapping):

    vin: str
    oem_name: str
    type: str
    vehicle_class: str
    mileage: str
    make: str
    model: str
    year: str
    new_or_used: str
    # dealer_integration_partner_id: Optional[str] = 'dealerCode'
