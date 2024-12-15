import json
import logging
from api_wrappers import PbsApiWrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_status_data(api_response):
    """
    Transforms the raw API response to be formatted.
    """
    parsed_data = []

    response_field = api_response.get('Response', [])
    status_list = response_field.get('SetupList', [])

    for status in status_list:
        parsed_status = {
            "Name": status.get("Name", ""),
            "SystemStatus": status.get("SystemStatus", ""),
            "Default": status.get("Lead", ""),
            "Inactive": status.get("Inactive", "")
        }
        parsed_data.append(parsed_status)

    return parsed_data


def lambda_handler(event, context):
    """Get dealer's salespersons list from PBS."""
    logger.info("Lambda function invoked with event: %s", event)

    api = PbsApiWrapper()
    crm_dealer_id = event.get("crm_dealer_id", "2004.QA")
    logger.info("Fetching employee data for DealerId: %s", crm_dealer_id)

    try:
        result = api.get_dealer_lead_statuses(crm_dealer_id)
        logger.info("Successfully received response from API: %s", result)
        parsed_result = parse_status_data(result)
        logger.info("Successfully parsed response from API to: %s", parsed_result)

    except Exception as e:
        logger.error("Failed to retrieve employee data: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps(parsed_result)
    }


# Main function for local testing
def main():
    # Dummy event to simulate an AWS Lambda event
    dummy_event = {
        "employee_id": "00000000000000000000000000000000"
    }

    # Since we're testing locally, context can be None
    response = lambda_handler(dummy_event, None)

    # Print the response to verify the output
    print("Response:", response)


if __name__ == "__main__":
    main()
