import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from typing import Any, List
from datetime import datetime
from utils import invoke_vendor_lambda, IntegrationError, format_timestamp, send_alert_notification

from appt_orm.session_config import DBSession
from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.op_code_product import OpCodeProduct
from appt_orm.models.op_code_appointment import OpCodeAppointment
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle
from appt_orm.models.appointment import Appointment

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['dealer_integration_partner_id', 'integration_consumer_id', 'product_consumer_id',
                  'first_name', 'last_name', 'email_address', 'phone_number']

ENVIRONMENT = environ.get("ENVIRONMENT", "test")


def update_attrs(db_object: Any, data: Any, allowed_attrs: List[str]) -> None:
    """Update attributes of a database object."""
    for attr in allowed_attrs:
        if attr in data:
            setattr(db_object, attr, data[attr])


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        body = loads(event["body"])
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        consumer_id = params.get("consumer_id")

        op_code = body["op_code"]
        timeslot = body["timeslot"]
        consumer = body.get("consumer", {})
        vehicle = body.get("vehicle", {})

        with DBSession() as session:
            # Get dealer info
            dealer_partner = session.query(
                DealerIntegrationPartner.id, DealerIntegrationPartner.product_id,
                DealerIntegrationPartner.integration_dealer_id,
                Dealer.timezone, IntegrationPartner.metadata_
            ).join(
                Dealer, Dealer.id == DealerIntegrationPartner.dealer_id
            ).join(
                IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
            ).filter(
                DealerIntegrationPartner.id == dealer_integration_partner_id,
                DealerIntegrationPartner.is_active == True
            ).first()

            if not dealer_partner:
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

            # Get vendor op code
            op_code_query = session.query(
                OpCode.op_code, OpCodeAppointment.id
            ).join(
                OpCodeAppointment, OpCodeAppointment.op_code_id == OpCode.id
            ).join(
                OpCodeProduct, OpCodeProduct.id == OpCodeAppointment.op_code_product_id
            ).filter(
                OpCode.dealer_integration_partner_id == dealer_integration_partner_id,
                OpCodeProduct.product_id == dealer_partner.product_id,
                OpCodeProduct.op_code == op_code
            ).first()

            if not op_code_query:
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No integration op code mapping found for product op code: {op_code}",
                        "request_id": request_id,
                    })
                }
            vendor_op_code = op_code_query.op_code
            appt_op_code_id = op_code_query.id
            logger.info(f"Product op code {op_code} mapped to vendor op code {vendor_op_code}")

        create_appt_arn = partner_metadata.get("create_appt_arn", "")
        if ENVIRONMENT == 'prod' and not create_appt_arn:
            raise Exception(f"CreateAppt ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "integration_dealer_id": integration_dealer_id,
            "dealer_timezone": dealer_timezone,
            "op_code": vendor_op_code,
            "timeslot": timeslot,
            "duration": body.get("timeslot_duration"),
            "comment": body.get("comment"),
            "first_name": consumer.get("first_name"),
            "last_name": consumer.get("last_name"),
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
                session.add(consumer_db)
                session.flush()
                consumer_id = consumer_db.id
                logger.info(f"Created consumer with id {consumer_id}")

            # Create vehicle in DB
            vehicle_db = Vehicle(
                consumer_id=consumer_id,
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
            session.add(vehicle_db)
            session.flush()
            vehicle_id = vehicle_db.id
            logger.info(f"Created vehicle with id {vehicle_id}")

            # Create appointment in DB
            appointment_db = Appointment(
                consumer_id=consumer_id,
                vehicle_id=vehicle_id,
                integration_appointment_id=integration_appointment_id,
                op_code_appointment_id=appt_op_code_id,
                timeslot_ts=format_timestamp(timeslot, dealer_timezone),
                timeslot_duration=body.get("timeslot_duration"),
                created_date_ts=body.get("created_date_ts", datetime.utcnow().isoformat()),
                status="Active",
                comment=body.get("comment")
            )

            session.add(appointment_db)
            session.flush()
            appointment_id = appointment_db.id
            logger.info(f"Created appointment with id {appointment_id}")
            session.commit()

        return {
            "statusCode": 201,
            "body": dumps({
                "appointment_id": int(appointment_id),
                "consumer_id": int(consumer_id),
                "vehicle_id": int(vehicle_id),
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
