from orm.models.carlabs import RepairOrder
from orm.models.shared_dms import Vehicle


def map_vehicle(record: RepairOrder, dip_id: int):
    vehicle = Vehicle()
    vehicle.vin = record.vin
    vehicle.make = record.make
    try:
        vehicle.year = int(record.year)
    except (ValueError, TypeError):
        vehicle.year = None
    vehicle.mileage = record.mileage
    vehicle.dealer_integration_partner_id = dip_id
    vehicle.metadata_column = {'repair_order_id': record.id, 'data_source': record.ro_source}
    return vehicle
