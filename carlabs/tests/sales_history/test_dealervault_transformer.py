from orm.models.shared_dms.vehicle_sale import VehicleSale
from orm.models.shared_dms.consumer import Consumer
from orm.models.shared_dms.vehicle import Vehicle
from orm.models.shared_dms.service_contract import ServiceContract


def test_vehicle_sale(dealervault_transformer):
    expected_values = {
        'dealer_id': 'yellowstone_country_motors-service',
        'sale_date': '03/29/2022',
        'listed_price': '',
        'sales_tax': '',
        'mileage_on_vehicle': '121275',
        'deal_type': 'P',
        'cost_of_vehicle': '1500.00',
        'oem_msrp': '',
        'adjustment_on_price': '',
        'date_of_state_inspection': None,
        'is_new': 'U',
        'trade_in_value': '',
        'payoff_on_trade': None,
        'value_at_end_of_lease': '',
        'miles_per_year': '1250',
        'profit_on_sale': '',
        'has_service_contract': False,
        'vehicle_gross': '2800.00',
        'warranty_expiration_date': '',
        'service_package_flag': True,
        'db_creation_date': '2022-05-25T17:26:30.798Z',
        'vin': 'JF1GH63638G835772',
        'make': 'SUBARU',
        'model': 'IMPREZA',
        'year': '2008',
        'delivery_date': '',
        'finance_rate': '0.0000',
        'finance_term': '1',
        'finance_amount': '2800.00',
        'date_of_inventory': '',
        'days_in_stock': None
    }
    assert isinstance(dealervault_transformer.vehicle_sale, VehicleSale)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealervault_transformer.vehicle_sale, attribute) == expected_value


def test_consumer(dealervault_transformer):
    expected_values = {
        'dealer_customer_no': '5S115435',
        'dealer_id': 'yellowstone_country_motors-service',
        'first_name': '',
        'last_name': 'WHOLESALE REMARKETING INC',
        'email': 'SHAWN@WHOLESALEREMARKETING.COM',
        'ip_address': None,
        'cell_phone': '4066716912',
        'city': 'BILLINGS',
        'state': 'MT',
        'metro': None,
        'postal_code': '59101-4217',
        'home_phone': '4066716912',
        'email_optin_flag': False,
        'phone_optin_flag': False,
        'postal_mail_optin_flag': True,
        'sms_optin_flag': False,
        'address': '2424 1ST AVE S',
        'db_creation_date': '2022-05-25T17:26:30.798Z',
    }
    assert isinstance(dealervault_transformer.consumer, Consumer)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealervault_transformer.consumer, attribute) == expected_value


def test_vehicle(dealervault_transformer):
    expected_values = {
        'dealer_id': 'yellowstone_country_motors-service',
        'vin': 'JF1GH63638G835772',
        'type': None,
        'mileage': '121275',
        'make': 'SUBARU',
        'model': 'IMPREZA',
        'year': '2008',
        'db_creation_date': '2022-05-25T17:26:30.798Z',
    }
    assert isinstance(dealervault_transformer.vehicle, Vehicle)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealervault_transformer.vehicle, attribute) == expected_value


def test_service_contract(dealervault_transformer):
    expected_values = {
        'dealer_id': 'yellowstone_country_motors-service',
        'contract_name': 'NONE',
        'start_date': '03/29/2022',
        'amount': '',
        'cost': '',
        'deductible': None,
        'expiration_months': '',
        'expiration_miles': '121275',
        'db_creation_date': '2022-05-25T17:26:30.798Z',
    }
    assert isinstance(dealervault_transformer.service_contract, ServiceContract)
    for attribute, expected_value in expected_values.items():
        assert getattr(dealervault_transformer.service_contract, attribute) == expected_value
