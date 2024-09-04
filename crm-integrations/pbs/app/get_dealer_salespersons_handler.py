import json
import logging
from api_wrappers import PbsApiWrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Get dealer's salespersons list from PBS."""
    logger.info("Lambda function invoked with event: %s", event)

    api = PbsApiWrapper()
    crm_dealer_id = event.get("crm_dealer_id", "00000000000000000000000000000000")
    employee_id = event.get("employee_id", "00000000000000000000000000000000")  # Default EmployeeId for testing
    logger.info("Fetching employee data for EmployeeId: %s", employee_id)

    try:
        result = api.call_employee_get(employee_id, crm_dealer_id)
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
