from orm.models.carlabs import RepairOrder
from orm.models.shared_dms import Consumer


def map_consumer(record: RepairOrder, dip_id: int):
    consumer = Consumer()
    consumer.email = record.email_address
    consumer.first_name = record.consumer_name
    consumer.cell_phone = record.cell_phone
    consumer.postal_code = record.consumer_zipcode
    consumer.email_optin_flag = record.email_optin_flag
    consumer.phone_optin_flag = record.phone_optin_flag
    consumer.dealer_integration_partner_id = dip_id
    consumer.metadata_column = {'repair_order_id': record.id, 'data_source': record.ro_source}
    return consumer
