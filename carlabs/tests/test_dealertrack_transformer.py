from app.transformers.dealertrack import DealertrackTransformer
from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer

# TODO check for content rightness

def test_vehicle_sale(dealertrack_data):
    transformer = DealertrackTransformer(carlabs_data=dealertrack_data)
    assert isinstance(transformer.vehicle_sale, VehicleSale)

def test_consumer(dealertrack_data):
    transformer = DealertrackTransformer(carlabs_data=dealertrack_data)
    assert isinstance(transformer.consumer, Consumer)