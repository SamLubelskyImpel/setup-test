from orm.models.shared_dms.vehicle_sale import VehicleSale
from orm.models.shared_dms.consumer import Consumer
from orm.models.shared_dms.vehicle import Vehicle
from orm.models.shared_dms.service_contract import ServiceContract


def test_vehicle_sale(dealertrack_transformer):
    expected_values = {
        'dealer_id': 'white_motors-chevrolet_buick_gmc',
        'sale_date': '20210316',
        'listed_price': '0.00',
        'sales_tax': '326.56',
        'mileage_on_vehicle': '52000',
        'deal_type': 'R',
        'cost_of_vehicle': '8500.00',
        'oem_msrp': '0.00',
        'adjustment_on_price': '0.00',
        'date_of_state_inspection': None,
        'is_new': 'U',
        'trade_in_value': None,
        'payoff_on_trade': '0.00',
        'value_at_end_of_lease': None,
        'miles_per_year': '12000',
        'profit_on_sale': None,
        'has_service_contract': True,
        'vehicle_gross': '0.00',
        'warranty_expiration_date': '0',
        'service_package_flag': True,
        'db_creation_date': '2022-04-26T12:55:43.030Z',
        'vin': 'WP0CA29822U621573',
        'make': 'PORSCHE',
        'model': 'BOXSTER',
        'year': '2002',
        'delivery_date': '20210316',
        'finance_rate': '8.500',
        'finance_term': '60',
        'finance_amount': '6108.56',
        'date_of_inventory': '20170928',
        'days_in_stock': 1265
    }
    assert isinstance(dealertrack_transformer.vehicle_sale, VehicleSale)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealertrack_transformer.vehicle_sale, attribute) == expected_value


def test_consumer(dealertrack_transformer):
    expected_values = {
        'dealer_id': 'white_motors-chevrolet_buick_gmc',
        'first_name': 'ANOTHER',
        'last_name': 'TESTER',
        'email': 'blair.keck@autopoint.com',
        'ip_address': None,
        'cell_phone': '1112223333',
        'city': 'DEERFIELD BEACH',
        'state': 'FL',
        'metro': None,
        'postal_code': '33441',
        'home_phone': '6267777979',
        'email_optin_flag': True,
        'phone_optin_flag': True,
        'postal_mail_optin_flag': True,
        'sms_optin_flag': True,
        'address': '1234 FORD RD',
        'db_creation_date': '2022-04-26T12:55:43.030Z',
    }
    assert isinstance(dealertrack_transformer.consumer, Consumer)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealertrack_transformer.consumer, attribute) == expected_value


def test_vehicle(dealertrack_transformer):
    expected_values = {
        'dealer_id': 'white_motors-chevrolet_buick_gmc',
        'vin': 'WP0CA29822U621573',
        'type': 'U',
        'mileage': '52000',
        'make': 'PORSCHE',
        'model': 'BOXSTER',
        'year': '2002',
        'db_creation_date': '2022-04-26T12:55:43.030Z',
    }
    assert isinstance(dealertrack_transformer.vehicle, Vehicle)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealertrack_transformer.vehicle, attribute) == expected_value


def test_service_contract(dealertrack_transformer):
    expected_values = {
        'dealer_id': 'white_motors-chevrolet_buick_gmc',
        'contract_name': 'The Service Contract',
        'start_date': '20210316',
        'amount': '5475.00',
        'cost': '1250.00',
        'deductible': '0',
        'expiration_months': '0',
        'expiration_miles': '0',
        'db_creation_date': '2022-04-26T12:55:43.030Z',
    }
    assert isinstance(dealertrack_transformer.service_contract, ServiceContract)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealertrack_transformer.service_contract, attribute) == expected_value
