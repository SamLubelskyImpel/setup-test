from orm.models.repair_order import RepairOrder
from orm.models.service_repair_order import ServiceRepairOrder
from orm.connection.session import SQLSession
from orm.models.consumer import Consumer
from orm.models.vehicle import Vehicle


def map_service_repair_order(record: RepairOrder):
    with SQLSession(db='SHARED_DMS') as carlabs_session:
        consumer = carlabs_session.query(Consumer).where(
            Consumer.email.ilike(record.email_address)).first()
        if not consumer:
            raise Exception(f'consumer {record.email_address} not found')
        consumer_id = consumer.id

        vehicle = carlabs_session.query(Vehicle).where(
            Vehicle.vin.ilike(record.vin)).first()
        if not vehicle:
            raise Exception(f'vehicle {record.vin} not found')
        vehicle_id = vehicle.id

    date_format = '%Y-%m-%d'
    ro = ServiceRepairOrder()
    ro.consumer_id = consumer_id
    ro.vehicle_id = vehicle_id
    ro.ro_open_date = record.ro_open_date
    ro.ro_close_date = record.ro_close_date
    ro.txn_pay_type = 'warranty' if record.warranty_flag else 'consumer'
    ro.repair_order_no = record.ro_number
    ro.total_amount = record.total_amount
    # TODO this field needs to be of type  TEXT
    # ro.comment = str(record.ro_service_details)
    ro.dealer_integration_partner_id = 1
    return ro
