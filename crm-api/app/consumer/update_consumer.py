"""Update consumer."""

import logging
from os import environ
from json import loads, dumps
from typing import Any

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update consumer."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        consumer_id = event["pathParameters"]["consumer_id"]

        fields_to_update = [
            "first_name", "last_name", "middle_name", "email", "phone",
            "email_optin_flag", "sms_optin_flag", "city", "country",
            "address", "postal_code"
        ]

        with DBSession() as session:
            consumer = session.query(Consumer).filter(Consumer.id == consumer_id).first()
            if not consumer:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found."})
                }

            for field in fields_to_update:
                if field in body:
                    setattr(consumer, field, body[field])

            session.commit()

        logger.info(f"Consumer updated {consumer_id}")

        return {
            "statusCode": 200,
            "body": dumps({"message": "Consumer updated successfully"})
        }

    except Exception as e:
        logger.error(f"Error updating consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
