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
from appt_orm.models.op_code import OpCode

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['dealer_integration_partner_id', 'first_name', 'last_name', 'email_address', 'phone_number']
required_params = ["timeslot"]

ENVIRONMENT = environ.get("ENVIRONMENT", "test")

# def get_field(current, incoming, field_name):
#     try:
#         new_value = incoming.get(field_name, None)

#         if not new_value:
#             return current.get(field_name)
        
#         return new_value

#     except Exception:
#         raise Exception(f"Unexpected field name: {field_name}")


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

            update_appointment = session.query(
                Appointment.integration_appointment_id, 
                Vehicle.vin, 
                Vehicle.make, 
                Vehicle.model, 
                Vehicle.manufactured_year, 
                Consumer.first_name, 
                Consumer.last_name, 
                Consumer.email_address, 
                Consumer.phone_number, 
                OpCode.op_code
                ).join(
                Consumer, Consumer.id == Appointment.consumer_id
                ).join(
                    Vehicle, Vehicle.id == Appointment.vehicle_id
                ).join(
                    OpCode,
                    OpCode.id == Appointment.op_code_id
                ).filter(
                    Appointment.id == appointment_id
                ).first()

        if not update_appointment:
            raise Exception(f"Appointment with ID {appointment_id} could not be found.")

        logger.info(f"UpdateAppointment: {update_appointment}")
        logger.info(f"Dealer metadata: {partner_metadata}")
        update_appt_arn = partner_metadata.get("update_appt_arn", "")
        if not update_appt_arn:
            raise Exception(f"UpdateAppt ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "integration_appointment_id": update_appointment.integration_appointment_id,
            "source_product": source_product,
            "integration_dealer_id": integration_dealer_id,
            "timeslot": timeslot,
            "duration": body.get("duration", 15),
            "first_name": update_appointment.first_name,
            "last_name": update_appointment.last_name,
            "email_address": update_appointment.email_address,
            "phone_number": update_appointment.phone_number,
            "op_code": update_appointment.op_code,
            "dealer_timezone": dealer_timezone,
            "vin": update_appointment.vin,
            "year": update_appointment.manufactured_year,
            "make": update_appointment.make,
            "model": update_appointment.model
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

        # # Parse response
        integration_appointment_id = update_appointment.integration_appointment_id

        with DBSession() as session:
            session.query(Appointment).filter(Appointment.id == appointment_id).update({"timeslot_ts": format_timestamp(timeslot, dealer_timezone)})
            session.commit()

        return {
            "statusCode": 204,
            "body": dumps({
                "message": "success"
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
