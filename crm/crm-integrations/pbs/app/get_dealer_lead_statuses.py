import json
import logging
from api_wrappers import PbsApiWrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Get dealer's lead statuses list from PBS."""
    logger.info("Lambda function invoked with event: %s", event)

    api = PbsApiWrapper()
    crm_dealer_id = event.get("crm_dealer_id")
    logger.info("Fetching employee data for DealerId: %s", crm_dealer_id)

    try:
        result = api.get_dealer_lead_statuses(crm_dealer_id)
        logger.info("Successfully received response from API: %s", result)

    except Exception as e:
        logger.error("Failed to retrieve employee data: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }