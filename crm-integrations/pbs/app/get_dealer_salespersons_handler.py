from pbs_api_wrapper import PartnerHubAPI
import json

def lambda_handler(event, context):
    api = PartnerHubAPI()

    employee_id = event.get("employee_id", "00000000000000000000000000000000")  # Default EmployeeId for testing
    result = api.call_employee_get(employee_id)

    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }

# Main function for local testing
def main():
    # Dummy event to simulate an AWS Lambda event
    dummy_event = {
        "EmployeeId": "00000000000000000000000000000000"  # Replace with a valid EmployeeId if available
    }

    # Since we're testing locally, context can be None
    response = lambda_handler(dummy_event, None)
    
    # Print the response to verify the output
    print("Response:", response)

if __name__ == "__main__":
    main()
