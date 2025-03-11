import json
import logging
from get_dealer_salespersons_handler import lambda_handler


# Caplog is one of the default fixtures that is included in pytest.
def test_get_dealer_salespersons(caplog):
    # Create a dummy event with the test dealer ID (all zeros)
    dummy_event = {
        "employee_id": "00000000000000000000000000000000",
        "crm_dealer_id": "2004.QA"
    }

    # Run the handler and capture logs
    with caplog.at_level(logging.INFO):
        response = lambda_handler(dummy_event, None)

    # Assertions to ensure the function ran successfully
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    print(body)