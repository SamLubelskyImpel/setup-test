from app.transformers.dealervault import DealervaultTransformer
from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer

# TODO check for content rightness

def test_vehicle_sale(dealervault_data):
    transformer = DealervaultTransformer(carlabs_data=dealervault_data)
    assert isinstance(transformer.vehicle_sale, VehicleSale)

def test_consumer(dealervault_data):
    transformer = DealervaultTransformer(carlabs_data=dealervault_data)
    assert isinstance(transformer.consumer, Consumer)