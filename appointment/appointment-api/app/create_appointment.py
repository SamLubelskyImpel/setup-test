"""Create an appointment in the vendor."""

import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from typing import Any, List
from datetime import datetime, timezone
from utils import (invoke_vendor_lambda, IntegrationError, format_timestamp,
                   send_alert_notification, get_dealer_info, get_vendor_op_code,
                   validate_request_body, ValidationError)

from appt_orm.session_config import DBSession
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle
from appt_orm.models.appointment import Appointment

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['dealer_integration_partner_id', 'first_name', 'last_name', 'email_address', 'phone_number']
required_params = ["op_code", "timeslot", {"consumer": ["first_name", "last_name"]}, "vehicle"]

ENVIRONMENT = environ.get("ENVIRONMENT", "test")


def update_attrs(db_object: Any, data: Any, allowed_attrs: List[str]) -> None:
    """Update attributes of a database object."""
    for attr in allowed_attrs:
        if attr in data:
            setattr(db_object, attr, data[attr])


def lambda_handler(event, context):
    """Create an appointment in the vendor for a service."""
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]
        body = loads(event["body"])
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        consumer_id = params.get("consumer_id")

        validate_request_body(body, required_params)

        op_code = body["op_code"]
        timeslot = body["timeslot"]
        consumer = body["consumer"]
        vehicle = body["vehicle"]

        # Optional parameter validation
        vin = vehicle.get("vin")
        year = vehicle.get("year")
        make = vehicle.get("make")
        model = vehicle.get("model")
        if not vin and not (year and make and model):
            raise ValidationError("VIN or Year, Make, Model must be provided")

        email_address = consumer.get("email_address")
        phone_number = consumer.get("phone_number")
        if not email_address and not phone_number:
            raise ValidationError("Email address or phone number must be provided")

        with DBSession() as session:
            dealer_partner = get_dealer_info(
                session,
                dealer_integration_partner_id,
                integration_partner
            )
            if not dealer_partner:
                logger.error(f"No active dealer found with id {dealer_integration_partner_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No active dealer found with id {dealer_integration_partner_id}",
                        "request_id": request_id,
                    })
                }
            logger.info(f"Dealer integration partner: {dealer_partner}")
            dealer_timezone = dealer_partner.timezone
            integration_dealer_id = dealer_partner.integration_dealer_id
            partner_metadata = dealer_partner.metadata_
            source_product = dealer_partner.product_name

            # Get vendor op code
            op_code_result = get_vendor_op_code(session, dealer_integration_partner_id, op_code, dealer_partner.product_id)
            if not op_code_result:
                logger.error(f"No integration op code mapping found for product op code: {op_code}")
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No integration op code mapping found for product op code: {op_code}",
                        "request_id": request_id,
                    })
                }
            vendor_op_code = op_code_result.op_code
            appt_op_code_id = op_code_result.id
            logger.info(f"Product op code {op_code} mapped to vendor op code {vendor_op_code}")

        create_appt_arn = partner_metadata.get("create_appt_arn", "")
        if not create_appt_arn:
            raise Exception(f"CreateAppt ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "source_product": source_product,
            "integration_dealer_id": integration_dealer_id,
            "dealer_timezone": dealer_timezone,
            "op_code": vendor_op_code,
            "timeslot": timeslot,
            "duration": body.get("timeslot_duration"),
            "comment": body.get("comment"),
            "first_name": consumer["first_name"],
            "last_name": consumer["last_name"],
            "email_address": consumer.get("email_address"),
            "phone_number": consumer.get("phone_number"),
            "vin": vehicle.get("vin"),
            "year": vehicle.get("year"),
            "make": vehicle.get("make"),
            "model": vehicle.get("model")
        }
        logger.info(f"Payload to integration: {payload}")

        response = invoke_vendor_lambda(payload, create_appt_arn)
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
        elif response["statusCode"] not in [200, 201]:
            raise IntegrationError(f"Vendor integration responded with status code {response['statusCode']}")

        # Parse response
        integration_appointment_id = loads(response["body"])["appointment_id"]

        # Create consumer
        with DBSession() as session:
            if consumer_id:
                consumer_db = session.query(Consumer).filter_by(id=consumer_id).first()
                if not consumer_db:
                    logger.warning(f"No consumer found with id {consumer_id}. Creating new consumer.")
                    consumer_id = None

            if not consumer_id:
                consumer_db = Consumer()
                update_attrs(
                    consumer_db,
                    {**consumer, "dealer_integration_partner_id": dealer_integration_partner_id},
                    consumer_attrs
                )
                logger.info("Consumer pending")

            # Create vehicle in DB
            vehicle_db = Vehicle(
                vin=vehicle.get("vin"),
                vehicle_class=vehicle.get("vehicle_class"),
                mileage=vehicle.get("mileage"),
                make=vehicle.get("make"),
                model=vehicle.get("model"),
                manufactured_year=vehicle.get("year"),
                body_style=vehicle.get("body_style"),
                transmission=vehicle.get("transmission"),
                interior_color=vehicle.get("interior_color"),
                exterior_color=vehicle.get("exterior_color"),
                trim=vehicle.get("trim"),
                condition=vehicle.get("condition"),
                odometer_units=vehicle.get("odometer_units")
            )
            vehicle_db.consumer = consumer_db

            logger.info("Vehicle pending")

            # Create appointment in DB
            appointment_db = Appointment(
                integration_appointment_id=integration_appointment_id,
                op_code_appointment_id=appt_op_code_id,
                timeslot_ts=format_timestamp(timeslot, dealer_timezone),
                timeslot_duration=body.get("timeslot_duration"),
                created_date_ts=body.get("created_date_ts", datetime.now(timezone.utc).isoformat()),
                status="Active",
                comment=body.get("comment")
            )
            appointment_db.vehicle = vehicle_db
            appointment_db.consumer = consumer_db

            session.add(appointment_db)
            session.commit()

            appointment_id = appointment_db.id
            logger.info(f"Created appointment with id {appointment_id}")

        return {
            "statusCode": 201,
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
        send_alert_notification(request_id, "CreateAppointment", e)
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
        send_alert_notification(request_id, "CreateAppointment", e)
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
