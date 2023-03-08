from session_config import DBSession
from models.vehicle import Vehicle


with DBSession() as session:
    vehicle = session.query(
        Vehicle
    ).filter(
        Vehicle.vin == 'WOEKQ'
    ).one()

    print(vehicle)
    print(vehicle.oem_name)