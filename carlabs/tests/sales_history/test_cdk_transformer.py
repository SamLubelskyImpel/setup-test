from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer
from app.orm.models.shared_dms.vehicle import Vehicle
from app.orm.models.shared_dms.service_contract import ServiceContract

# TODO check for content rightness

def test_vehicle_sale(cdk_transformer):
    assert isinstance(cdk_transformer.vehicle_sale, VehicleSale)


def test_consumer(cdk_transformer):
    assert isinstance(cdk_transformer.consumer, Consumer)

def test_vehicle(cdk_transformer):
    assert isinstance(cdk_transformer.vehicle, Vehicle)

def test_service_contract(cdk_transformer):
    assert isinstance(cdk_transformer.service_contract, ServiceContract)