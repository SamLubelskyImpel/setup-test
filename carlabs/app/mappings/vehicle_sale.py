from dataclasses import dataclass
from typing import Optional
from .base import BaseMapping


@dataclass
class VehicleSaleTableMapping(BaseMapping):

    sale_date: str
    listed_price: str
    sales_tax: str
    mileage_on_vehicle: str
    deal_type: str
    cost_of_vehicle: str
    oem_msrp: str
    adjustment_on_price: str
    date_of_state_inspection: str
    is_new: str
    trade_in_value: str
    payoff_on_trade: str
    value_at_end_of_lease: str
    miles_per_year: str
    profit_on_sale: str
    vehicle_gross: str
    warranty_expiration_date: str
    vin: str
    make: str
    model: str
    year: str
    delivery_date: str
    finance_rate: str
    finance_term: str
    finance_amount: str
    date_of_inventory: str
    has_service_contract: str
    service_package_flag: str
    dealer_id: Optional[str] = 'dealerCode'
    db_creation_date: Optional[str] = 'creationDate'