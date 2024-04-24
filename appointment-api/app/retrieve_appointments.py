import logging
from os import environ
from json import dumps, loads, JSONEncoder
from uuid import uuid4
from decimal import Decimal
from datetime import datetime, timezone
from utils import invoke_vendor_lambda, IntegrationError, convert_utc_to_timezone, send_alert_notification
from typing import Any

from appt_orm.session_config import DBSession
from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.op_code_product import OpCodeProduct
from appt_orm.models.op_code_appointment import OpCodeAppointment
from appt_orm.models.appointment import Appointment
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "test")


class CustomEncoder(JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects."""

    def default(self, obj: Any) -> Any:
        """Serialize datetime and Decimal objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super(CustomEncoder, self).default(obj)


def update_appointment_status(appointment_id, session, new_status):
    session.query(Appointment).filter(Appointment.id == appointment_id).update({"status": new_status})


def generate_appointment_response(appointment):
    return {
        "id": appointment["id"],
        "op_code": appointment["op_code"],
        "timeslot": appointment["timeslot"],
        "timeslot_duration": appointment["timeslot_duration"],
        "comment": appointment["comment"],
        "status": appointment["status"],
        "consumer": {
            "first_name": appointment["consumer"]["first_name"],
            "last_name": appointment["consumer"]["last_name"],
            "email_address": appointment["consumer"]["email_address"],
            "phone_number": appointment["consumer"]["phone_number"],
        },
        "vehicle": {
            "vin": appointment["vehicle"]["vin"],
        }
    }


def get_product_op_code(dealer_integration_partner_id, product_id, integration_op_code):
    with DBSession() as session:
        product_op_code = session.query(
            OpCodeProduct
        ).join(
            OpCodeAppointment, OpCodeAppointment.op_code_product_id == OpCodeProduct.id
        ).join(
            OpCode, OpCode.id == OpCodeAppointment.id
        ).filter(
            OpCodeProduct.product_id == product_id,
            OpCode.dealer_integration_partner_id == dealer_integration_partner_id,
            OpCode.op_code == integration_op_code
        ).first()

    return product_op_code.op_code if product_op_code else None


def extract_appt_data(db_appt, dealer_timezone, dealer_integration_partner_id):
    return {
        "id": db_appt.Appointment.id,
        "op_code": db_appt.op_code,
        "timeslot": convert_utc_to_timezone(db_appt.Appointment.timeslot_ts, dealer_timezone, dealer_integration_partner_id),
        "timeslot_duration": db_appt.Appointment.timeslot_duration,
        "created_date_ts": db_appt.Appointment.created_date_ts,
        "comment": db_appt.Appointment.comment,
        "status": db_appt.Appointment.status,
        "consumer": {
            "product_consumer_id": db_appt.Consumer.product_consumer_id,
            "first_name": db_appt.Consumer.first_name,
            "last_name": db_appt.Consumer.last_name,
            "email_address": db_appt.Consumer.email_address,
            "phone_number": db_appt.Consumer.phone_number,
        },
        "vehicle": {
            "vin": db_appt.Vehicle.vin,
            "make": db_appt.Vehicle.make,
            "model": db_appt.Vehicle.model,
            "year": db_appt.Vehicle.manufactured_year,
            "vehicle_class": db_appt.Vehicle.vehicle_class,
            "mileage": db_appt.Vehicle.mileage,
            "body_style": db_appt.Vehicle.body_style,
            "transmission": db_appt.Vehicle.transmission,
            "interior_color": db_appt.Vehicle.interior_color,
            "exterior_color": db_appt.Vehicle.exterior_color,
            "trim": db_appt.Vehicle.trim,
            "condition": db_appt.Vehicle.condition,
            "odometer_units": db_appt.Vehicle.odometer_units
        }
    }


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        vin = params["vin"]
        status = params.get("status")
        first_name = params.get("first_name")
        last_name = params.get("last_name")
        email_address = params.get("email_address")
        phone_number = params.get("phone_number")

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
            product_id = dealer_partner.product_id

            # Retrieve appointments
            filters = {
                "first_name": first_name,
                "last_name": last_name,
                "email_address": email_address,
                "phone_number": phone_number
            }

            appointments_query = session.query(
                Appointment, Consumer, Vehicle, OpCodeProduct.op_code
            ).join(
                Consumer, Consumer.id == Appointment.consumer_id
            ).join(
                Vehicle, Vehicle.id == Appointment.vehicle_id
            ).join(
                OpCodeAppointment, OpCodeAppointment.id == Appointment.op_code_appointment_id
            ).join(
                OpCodeProduct, OpCodeProduct.id == OpCodeAppointment.op_code_product_id
            ).filter(
                Consumer.dealer_integration_partner_id == dealer_integration_partner_id,
                Vehicle.vin == vin
            )
            for field, value in filters.items():
                if value:
                    appointments_query = appointments_query.filter(getattr(Consumer, field) == value)
                    logger.info(f"Filtering by {field}: {value}")

            appointments_db = appointments_query.all()
            logger.info(f"Appointments found: {len(appointments_db)}")

        # Retrieve appointments from vendor
        retrieve_appts_arn = partner_metadata.get("retrieve_appts_arn", "")
        if ENVIRONMENT == 'prod' and not retrieve_appts_arn:
            raise Exception(f"RetrieveAppts ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "integration_dealer_id": integration_dealer_id,
            "dealer_timezone": dealer_timezone,
            "first_name": first_name,
            "last_name": last_name,
            "email_address": email_address,
            "phone_number": phone_number,
            "vin": vin
        }
        logger.info(f"Payload to integration: {payload}")

        response = invoke_vendor_lambda(payload, retrieve_appts_arn)
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
        elif response["statusCode"] != 200:
            raise IntegrationError(f"Vendor integration responded with status code {response['statusCode']}")

        # Parse response
        logger.info(f"Response from integration: {response}")
        body = loads(response["body"])
        vendor_appointments = body["appointments"]

        appointments = []
        # Compare appointments from vendor with database records
        with DBSession() as session:
            for db_appt in appointments_db:
                for v_appt in vendor_appointments:
                    # Appointment found in vendor
                    if db_appt.Appointment.integration_appointment_id == v_appt["appointment_id"]:
                        if v_appt.get("status") and db_appt.Appointment.status != v_appt["status"]:
                            # Status change in vendor, update appointment in db
                            update_appointment_status(db_appt.Appointment.id, session, v_appt["status"])
                            db_appt.Appointment.status = v_appt["status"]
                            logger.info(f"Appointment {db_appt.Appointment.id} status updated to {v_appt['status']}")
                        appointments.append(extract_appt_data(db_appt, dealer_timezone, dealer_integration_partner_id))
                        break
                else:
                    # Appointment not found in vendor
                    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                    timeslot_time = db_appt.Appointment.timeslot_ts
                    if timeslot_time < current_time:
                        # Appointment timeslot has passed
                        logger.info(f"Appointment {db_appt.Appointment.id} not found in vendor, but timeslot has passed. Assumed Closed.")
                        update_appointment_status(db_appt.Appointment.id, session, "Closed")
                        db_appt.Appointment.status = "Closed"
                    else:
                        logger.info(f"Appointment {db_appt.Appointment.id} not found in vendor and timeslot has not passed. Assumed Lost.")
                        update_appointment_status(db_appt.Appointment.id, session, "Lost")
                        db_appt.Appointment.status = "Lost"

                    appointments.append(extract_appt_data(db_appt, dealer_timezone, dealer_integration_partner_id))
            session.commit()

        # Add appointments from vendor which weren't in database to response
        for appt in vendor_appointments:
            for db_appt in appointments_db:
                if appt["appointment_id"] == db_appt.Appointment.integration_appointment_id:
                    break
            else:
                appointment = {
                    "op_code": get_product_op_code(dealer_integration_partner_id, product_id, appt["op_code"]),
                    "timeslot": appt["timeslot"],
                    "timeslot_duration": appt["timeslot_duration"],
                    "comment": appt.get("comment"),
                    "status": appt["status"] if appt.get("status") else "Active",
                    "consumer": {
                        "first_name": appt.get("first_name"),
                        "last_name": appt.get("last_name"),
                        "email_address": appt.get("email_address"),
                        "phone_number": appt.get("phone_number"),
                    },
                    "vehicle": {
                        "vin": appt.get("vin"),
                    }
                }
                appointments.append(appointment)

        if status:
            appointments = [appt for appt in appointments if appt["status"] == status]

        logger.info(f"Appointments: {appointments}")
        return {
            "statusCode": 200,
            "body": dumps({
                "appointments": appointments,
                "request_id": request_id,
            }, cls=CustomEncoder)
        }

    except IntegrationError as e:
        logger.error(f"Integration error: {e}")
        send_alert_notification(request_id, "RetrieveAppointments", e)
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
        send_alert_notification(request_id, "RetrieveAppointments", e)
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
