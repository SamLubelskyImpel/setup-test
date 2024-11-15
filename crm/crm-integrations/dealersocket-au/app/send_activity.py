from typing import Any
from os import environ
from json import loads, dumps
from boto3 import client
from logging import getLogger
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from api_wrappers import CrmApiWrapper, DealersocketAUApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")

crm_api = CrmApiWrapper()


def get_salespersons_handler(event: Any, context: Any) -> Any:
    logger.info(f"This is event: {event}")
    try:
        formatted_salespersons = []
        dealersocket_au_crm_api = DealersocketAUApiWrapper(activity=event)
        salespersons = dealersocket_au_crm_api.get_salespersons()
        for person in salespersons:
            position_name = person.get("jobTitle")
            if not position_name:
                position_name = person.get("roles", [""])[0]
            formatted_salespersons.append(
                {
                    "crm_salesperson_id": person["employeeApiID"],
                    "first_name": person["firstName"],
                    "last_name": person.get("lastName"),
                    "position_name": position_name,
                }
            )
        
        return {"statusCode": 200, "body": dumps(formatted_salespersons)}

    except Exception as e:
        logger.exception(
            f"Failed to retrieve salespersons {event} to Dealersocket AU"
        )
        logger.error(
            f"[SUPPORT ALERT] Failed to Get salespersons [CONTENT] DealerIntegrationPartnerId: {event}"
        )
        return {"statusCode": 500, "error":"Failed to retrieve salespersons"}


def record_handler(record: SQSRecord):
    """Create activity on Dealersocket AU."""
    logger.info(f"Record: {record}")
    try:
        activity = record.json_body
        salesperson = crm_api.get_salesperson(activity["lead_id"])
        #TODO Delete or uncomment below code
        # if not salesperson and activity.get("activity_type", "") == "appointment":
        #     logger.warning(f"No salespersons found for lead_id: {activity['lead_id']}. Required for appointment activity.")
        #     return

        logger.info(f"Activity: {activity}, Salesperson: {salesperson}")

        dealersocket_au_crm_api = DealersocketAUApiWrapper(activity=activity, salesperson=salesperson)

        dealersocket_au_activity_id = dealersocket_au_crm_api.create_activity()
        logger.info(f"Dealersocket AU responded with activity ID: {dealersocket_au_activity_id}")

        crm_api.update_activity(activity["activity_id"], dealersocket_au_activity_id)

    except Exception as e:
        if "No existing scheduled appointments found" in str(e):
            logger.error(f"Error: {e}")
            return 
        logger.exception(f"Failed to post activity {activity['activity_id']} to Dealersocket AU")
        logger.error("[SUPPORT ALERT] Failed to Send Activity [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nActivityId: {}\nActivityType: {}\nTraceback: {}".format(
            activity["dealer_integration_partner_id"], activity["lead_id"], activity["activity_id"], activity["activity_type"], e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity on Dealersocket AU."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        return process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
