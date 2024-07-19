from orm.models.carlabs import DataImports
from orm.models.shared_dms import Vehicle
from utils import parsed_int, parsed_date

def map_vehicle(record: DataImports, dip_id: int):
    vehicle = Vehicle()
    imported_data = record.importedData

    if record.dataSource == 'CDK':
        vehicle.vin = imported_data['VIN'][0]
        vehicle.mileage = parsed_int(imported_data['VehicleMileage'][0])
        vehicle.make = imported_data['MakeName'][0]
        vehicle.model = imported_data['ModelName'][0]
        vehicle.year = parsed_date('year', imported_data['Year'][0])
        vehicle.new_or_used = 'N' if imported_data['FIDealType'][0] == 'New' else 'U'
    elif record.dataSource == 'DealerTrack':
        vehicle.vin = imported_data['VIN']
        vehicle.type = imported_data['VehicleType']
        vehicle.mileage = parsed_int(imported_data['vehicle']['Odometer'])
        vehicle.make = imported_data['Make']
        vehicle.model = imported_data['Model']
        vehicle.year = parsed_date('year', imported_data['ModelYear'])
        vehicle.new_or_used = imported_data['VehicleType']
    elif record.dataSource == 'DEALERVAULT':
        vehicle.vin = imported_data['VIN']
        vehicle.mileage = parsed_int(imported_data['Mileage'])
        vehicle.make = imported_data['Make']
        vehicle.model = imported_data['Model']
        vehicle.year = parsed_date('year', imported_data['Year'])
        vehicle.new_or_used = 'N' if imported_data['New/Used'] == 'NEW' else 'U'

    vehicle.dealer_integration_partner_id = dip_id
    vehicle.metadata_column = {'data_imports_id': record.id, 'data_source': record.dataSource}

    return vehicle
