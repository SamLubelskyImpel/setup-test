from orm.models.carlabs import RepairOrder
from orm.models.shared_dms import ServiceRepairOrder


def map_service_repair_order(record: RepairOrder, dip_id: int):
    ro = ServiceRepairOrder()
    ro.ro_open_date = record.ro_open_date
    ro.ro_close_date = record.ro_close_date
    ro.txn_pay_type = 'warranty' if record.warranty_flag else 'consumer'
    ro.repair_order_no = record.ro_number
    ro.total_amount = record.total_amount
    # TODO this field needs to be of type  TEXT
    # ro.comment = str(record.ro_service_details)
    ro.dealer_integration_partner_id = dip_id
    ro.metadata_column = {'repair_order_id': record.id, 'data_source': record.ro_source}
    return ro
