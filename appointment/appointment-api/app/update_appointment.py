"""Update an appointment in the vendor."""

import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from typing import Any, List
from datetime import datetime, timezone
from utils import (invoke_vendor_lambda, IntegrationError, format_timestamp,
                   send_alert_notification, get_dealer_info, get_vendor_op_code,
                   validate_request_body, ValidationError, is_valid_timezone)

from appt_orm.session_config import DBSession
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle
from appt_orm.models.appointment import Appointment

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

required_params = ["appointment_id", "timeslot"]

ENVIRONMENT = environ.get("ENVIRONMENT", "test")

# def update_attrs(db_object: Any, data: Any, allowed_attrs: List[str]) -> None:
#     """Update attributes of a database object."""
#     for attr in allowed_attrs:
#         if attr in data:
#             setattr(db_object, attr, data.get(attr, None))

def lambda_handler(event, context):
    """Update an appointment in the vendor for a service."""
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        request_product = event["requestContext"]["authorizer"]["request_product"]
        body = loads(event["body"])
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]

        validate_request_body(body, required_params)

        appointment_id = params["appointment_id"]

        timeslot = body["timeslot"]

        vehicle = body.get('vehicle', {})

        update_appointment = {}

        with DBSession() as session:
            dealer_partner = get_dealer_info(
                session,
                dealer_integration_partner_id,
                request_product
            )
            if not dealer_partner:
                logger.error(f"No active dealer found with id {dealer_integration_partner_id} assigned to product {request_product}")
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No active dealer found with id {dealer_integration_partner_id} assigned to product {request_product}",
                        "request_id": request_id,
                    })
                }

            logger.info(f"Dealer integration partner: {dealer_partner}")
            dealer_timezone = dealer_partner.timezone
            integration_dealer_id = dealer_partner.integration_dealer_id
            partner_metadata = dealer_partner.metadata_
            source_product = dealer_partner.product_name

            if not is_valid_timezone(dealer_timezone):
                raise ValidationError("Invalid dealer timezone provided")

            update_appointment = session.query(Appointment).filter(Appointment.id == appointment_id).first()
        
        if not update_appointment:
            raise Exception(f"Appointment with ID {appointment_id} could not be found.")

        update_appt_arn = partner_metadata.get("update_appt_arn", "")
        if not update_appt_arn:
            raise Exception(f"UpdateAppt ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "appointment_id": update_appointment.integration_appointment_id,
            "source_product": source_product,
            "integration_dealer_id": integration_dealer_id,
            "timeslot": timeslot,
            "vin": vehicle.get("vin", None),
            "year": vehicle.get("year", None),
            "make": vehicle.get("make", None),
            "model": vehicle.get("model", None)
        }
        payload = {key: value for key, value in payload.items() if value}
        logger.info(f"Payload to integration: {payload}")

        response = invoke_vendor_lambda(payload, update_appt_arn)
        if response["statusCode"] == 500:
            logger.error(f"Integration encountered error: {response}")
            body = loads(response["body"])
            return {
                "statusCode": 500,
                "body": dumps({
                    "error": {
                        "code": body["error"]["code"],
                        "message": body["error"]["message"]
                    },
                    "request_id": request_id,
                })
            }
        elif response["statusCode"] not in [200, 204]:
            raise IntegrationError(f"Vendor integration responded with status code {response['statusCode']}")

        # Parse response
        integration_appointment_id = update_appointment.integration_appointment_id

        return {
            "statusCode": 204,
            "body": dumps({
                "appointment_id": int(appointment_id),
                "integration_appointment_id": str(integration_appointment_id) if integration_appointment_id else None,
                "consumer_id": int(appointment_db.consumer_id),
                "request_id": request_id,
            })
        }

    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return {
            "statusCode": 400,
            "body": dumps({
                "error": str(e),
                "request_id": request_id,
            })
        }
    except IntegrationError as e:
        logger.error(f"Integration error: {e}")
        send_alert_notification(request_id, "UpdateAppointment", e)
        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": "I002",
                    "message": "Unexpected response from vendor integration. Please contact Impel support."
                },
                "request_id": request_id,
            })
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        send_alert_notification(request_id, "UpdateAppointment", e)
        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": "I001",
                    "message": "Internal server error. Please contact Impel support."
                },
                "request_id": request_id,
            })
        }
