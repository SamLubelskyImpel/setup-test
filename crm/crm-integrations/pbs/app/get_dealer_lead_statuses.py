import json
import logging
from api_wrappers import PbsApiWrapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# def parse_employee_data(api_response):
#     """
#     Transforms the raw API response to the format required by the OAS.
#     """
#     parsed_data = []

#     # Extract the 'Employees' list from the API response
#     employees = api_response.get('Employees', [])

#     for employee in employees:
#         if not employee.get('Sales'):
#             continue
#         # Extract and transform relevant fields
#         parsed_employee = {
#             "email": [employee.get("EmailAddress", "")],
#             "first_name": employee.get("FirstName", ""),
#             "last_name": employee.get("LastName", ""),
#             "phone": [phone for phone in [employee.get("Phone", ""), employee.get("CellPhone", "")] if phone],
#             "crm_salesperson_id": employee.get("EmployeeId", ""),
#             "position_name": employee.get("SalesRole", "")
#         }

#         parsed_data.append(parsed_employee)

#     return parsed_data


def lambda_handler(event, context):
    """Get dealer's salespersons list from PBS."""
    logger.info("Lambda function invoked with event: %s", event)

    api = PbsApiWrapper()
    crm_dealer_id = event.get("crm_dealer_id", "2004.QA")
    logger.info("Fetching employee data for DealerId: %s", crm_dealer_id)

    try:
        result = api.get_dealer_lead_statuses(crm_dealer_id)
        logger.info("Successfully received response from API: %s", result)
        # parsed_result = parse_employee_data(result)
        # logger.info("Successfully parsed response from API to: %s", parsed_result)

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
