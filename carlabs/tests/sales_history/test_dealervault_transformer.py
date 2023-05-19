from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer
from app.orm.models.shared_dms.vehicle import Vehicle
from app.orm.models.shared_dms.service_contract import ServiceContract



# TODO check for content rightness

def test_vehicle_sale(dealervault_transformer):
    assert isinstance(dealervault_transformer.vehicle_sale, VehicleSale)

def test_consumer(dealervault_transformer):
    assert isinstance(dealervault_transformer.consumer, Consumer)

def test_vehicle(dealervault_transformer):
    assert isinstance(dealervault_transformer.vehicle, Vehicle)

def test_service_contract(dealervault_transformer):
    assert isinstance(dealervault_transformer.service_contract, ServiceContract)