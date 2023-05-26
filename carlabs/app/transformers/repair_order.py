from mappings import ServiceRepairOrderTableMapping
from pandas import DataFrame, json_normalize
from orm.models.shared_dms import ServiceRepairOrder
from datetime import datetime, date


class RepairOrderTransformer:

    carlabs_data: DataFrame
    mapping = ServiceRepairOrderTableMapping(
        ro_open_date='ro_open_date',
        ro_close_date='ro_close_date',
        txn_pay_type='warranty_flag',
        repair_order_no='ro_number',
        advisor_name=None,
        total_amount='total_amount',
        consumer_total_amount=None,
        warranty_total_amount=None,
        comment='ro_service_details',
        recommendation=None
    )

    date_format = '%Y-%m-%d'

    def __init__(self, carlabs_data: dict) -> None:
        self.carlabs_data = json_normalize(carlabs_data).iloc[0]

    def parsed_date(self, raw_date: str) -> date:
        try:
            return datetime.strptime(raw_date, self.date_format)
        except Exception:
            return None

    @property
    def repair_order(self):
        mapped = { dms_field: self.carlabs_data.get(carlabs_field) for dms_field, carlabs_field in self.mapping.fields }
        orm = ServiceRepairOrder(**mapped)
        orm.ro_open_date = self.parsed_date(orm.ro_open_date)
        orm.ro_close_date = self.parsed_date(orm.ro_close_date)
        orm.txn_pay_type = 'warranty' if orm.txn_pay_type else 'consumer'
        return orm
