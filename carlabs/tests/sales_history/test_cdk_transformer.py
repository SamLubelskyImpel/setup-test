from app.orm.models.shared_dms.vehicle_sale import VehicleSale
from app.orm.models.shared_dms.consumer import Consumer
from app.orm.models.shared_dms.vehicle import Vehicle
from app.orm.models.shared_dms.service_contract import ServiceContract


def test_vehicle_sale(cdk_transformer):
    expected_values = {
        'dealer_id': 'dealer-code',
        'sale_date': '2023-01-04',
        'listed_price': '27709.24',
        'sales_tax': '1329.24',
        'mileage_on_vehicle': '10003',
        'deal_type': 'Cash',
        'cost_of_vehicle': '22327.50',
        'oem_msrp': '',
        'adjustment_on_price': '0.00',
        'days_in_stock': None,
        'date_of_state_inspection': None,
        'is_new': 'Used',
        'trade_in_value': '2000.00',
        'payoff_on_trade': None,
        'value_at_end_of_lease': '',
        'miles_per_year': '',
        'profit_on_sale': '2072.50',
        'has_service_contract': False,
        'vehicle_gross': '23900.00',
        'warranty_expiration_date': None,
        'service_package_flag': False,
        'db_creation_date': '2023-01-05T12:00:37.648Z',
        'vin': '2GNAXJEV2L6188291',
        'make': 'CHEVROLET',
        'model': 'EQUINOX',
        'year': '2020',
        'delivery_date': '',
        'finance_rate': '',
        'finance_term': '',
        'finance_amount': '',
        'date_of_inventory': None,
        'si_load_process': '',
    }
    assert isinstance(cdk_transformer.vehicle_sale, VehicleSale)
    for attribute, expected_value in expected_values.items():
        assert getattr(cdk_transformer.vehicle_sale, attribute) == expected_value


def test_consumer(cdk_transformer):
    expected_values = {
        'dealer_id': 'dealer-code',
        'first_name': 'Robert',
        'last_name': 'Bernat',
        'email': 'rbernat7@gmail.com',
        'cell_phone': '2487562565',
        'state': 'MI',
        'postal_code': '48127-1398',
        'home_phone': '2487562565',
        'email_optin_flag': True,
        'phone_optin_flag': True,
        'postal_mail_optin_flag': True,
        'sms_optin_flag': True,
        'address': '8643 WINSTON LN',
        'db_creation_date': '2023-01-05T12:00:37.648Z',
    }
    assert isinstance(cdk_transformer.consumer, Consumer)
    for attribute, expected_value in expected_values.items():
        assert getattr(cdk_transformer.consumer, attribute) == expected_value


def test_vehicle(cdk_transformer):
    expected_values = {
        'dealer_id': 'dealer-code',
        'vin': '2GNAXJEV2L6188291',
        'mileage': '10003',
        'make': 'CHEVROLET',
        'model': 'EQUINOX',
        'year': '2020',
        'db_creation_date': '2023-01-05T12:00:37.648Z',
    }
    assert isinstance(cdk_transformer.vehicle, Vehicle)
    for attribute, expected_value in expected_values.items():
        assert getattr(cdk_transformer.vehicle, attribute) == expected_value


def test_service_contract(cdk_transformer):
    expected_values = {
        'dealer_id': 'dealer-code',
        'contract_name': '',
        'start_date': '2023-01-04',
        'amount': '',
        'cost': '0.00',
        'deductible': '',
        'expiration_months': '',
        'expiration_miles': '',
        'db_creation_date': '2023-01-05T12:00:37.648Z',
    }
    assert isinstance(cdk_transformer.service_contract, ServiceContract)
    for attribute, expected_value in expected_values.items():
        assert getattr(cdk_transformer.service_contract, attribute) == expected_value
