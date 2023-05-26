from orm.models.shared_dms import ServiceRepairOrder
from datetime import datetime

def test_repair_order(repair_order_transformer):
    expected_values = {
        'ro_open_date': datetime(2021, 11, 29, 0, 0),
        'ro_close_date': datetime(2021, 11, 30, 0, 0),
        'txn_pay_type': 'warranty',
        'repair_order_no': '601828',
        'advisor_name': None,
        'total_amount': None,
        'consumer_total_amount': None,
        'warranty_total_amount': None,
        'comment': '{\"6 Qt Oil Change and Rotation (Gas)|6 Qt Oil Change and Rotation (Gas)|PERFORM CERTIFIED MULTI-POINT VEHICLE INSPECTION|PERFORM CERTIFIED MULTI-POINT VEHICLE INSPECTION\"}',
        'recommendation': None,
    }
    assert isinstance(repair_order_transformer.repair_order, ServiceRepairOrder)
    for attribute, expected_value in expected_values.items():
        assert getattr(repair_order_transformer.repair_order, attribute) == expected_value