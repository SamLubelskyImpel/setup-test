import json
import logging
from api_wrappers import PbsApiWrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_status_data(api_response):
    """
    Transforms the raw API response to be formatted.
    """
    statuses_dict = {"generic_statuses": [], "custom_statuses": []}

    response_field = api_response.get('Response', {})
    status_list = response_field.get('SetupList', [])
    custom_statuses = [s.get("Name", "") for s in status_list]

    statuses_dict["custom_statuses"] = custom_statuses

    return statuses_dict


def lambda_handler(event, context):
    """Get dealer's lead statuses list from PBS."""
    logger.info("Lambda function invoked with event: %s", event)

    api = PbsApiWrapper()
    crm_dealer_id = event.get("crm_dealer_id")
    logger.info("Fetching employee data for DealerId: %s", crm_dealer_id)
    parsed_result = {}

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