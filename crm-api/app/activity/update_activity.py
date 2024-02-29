"""Update activity."""

import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.activity import Activity
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update activity."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        activity_id = event["pathParameters"]["activity_id"]

        crm_activity_id = body["crm_activity_id"]

        with DBSession() as session:
            # Check activity existence
            activity = session.query(Activity).filter(Activity.id == activity_id).first()
            if not activity:
                logger.error(f"Activity {activity_id} not found. Activity failed to be updated.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Activity {activity_id} not found. Activity failed to be updated."})
                }

            # Update activity
            activity.crm_activity_id = crm_activity_id
            session.commit()

            logger.info(f"Request product: {request_product} has updated activity {activity_id}")

        if request_product == "chat_ai":
            #TODO send this data to adf assembler
            pass

        return {
            "statusCode": "200",
            "body": dumps({"message": "Activity updated successfully"})
        }

    except Exception as e:
        logger.error(f"Error updating activity: {str(e)}")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
