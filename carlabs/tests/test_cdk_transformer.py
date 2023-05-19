from app.transformers.cdk import CDKTransformer
from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer

# TODO check for content rightness

def test_vehicle_sale(cdk_data):
    transformer = CDKTransformer(carlabs_data=cdk_data)
    assert isinstance(transformer.vehicle_sale, VehicleSale)


def test_consumer(cdk_data):
    transformer = CDKTransformer(carlabs_data=cdk_data)
    assert isinstance(transformer.consumer, Consumer)