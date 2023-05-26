from dataclasses import dataclass
from .base import BaseMapping

@dataclass
class ServiceRepairOrderTableMapping(BaseMapping):

    ro_open_date: str
    ro_close_date: str
    txn_pay_type: str
    repair_order_no: str
    advisor_name: str
    total_amount: str
    consumer_total_amount: str
    warranty_total_amount: str
    comment: str
    recommendation: str