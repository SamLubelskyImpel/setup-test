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
    # TODO can it be in the BaseMapping?
    dealer_id: Optional[str] = 'dealerCode'
    db_creation_date: Optional[str] = 'creationDate'
