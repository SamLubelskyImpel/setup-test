import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from datetime import datetime
from rds_instance import RDSInstance
from utils import invoke_vendor_lambda, IntegrationError, convert_utc_to_timezone

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


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


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        vin = params["vin"]
        op_code = params.get("op_code")
        status = params.get("status")
        first_name = params.get("first_name")
        last_name = params.get("last_name")
        email_address = params.get("email_address")
        phone_number = params.get("phone_number")

        # Get dealer info
        rds_instance = RDSInstance()
        dealer_partner = rds_instance.get_dealer_integration_partner(dealer_integration_partner_id)
        if not dealer_partner:
            return {
                "statusCode": "404",
                "body": dumps({
                    "error": f"No active dealer found with id {dealer_integration_partner_id}",
                    "request_id": request_id,
                })
            }
        logger.info(f"Dealer integration partner: {dealer_partner}")

        product_id = dealer_partner["product_id"]
        dealer_timezone = dealer_partner["timezone"]
        integration_dealer_id = dealer_partner["integration_dealer_id"]
        partner_metadata = dealer_partner["metadata"]

        # Get vendor op code
        appt_op_code = None
        # if op_code:
        #     vendor_op_code, appt_op_code = rds_instance.get_vendor_op_code(dealer_integration_partner_id, op_code, product_id)
        #     if not vendor_op_code:
        #         return {
        #             "statusCode": "404",
        #             "body": dumps({
        #                 "error": f"No integration op code mapping found for product op code: {op_code}",
        #                 "request_id": request_id,
        #             })
        #         }

        # Retrieve appointments
        db_appointments = rds_instance.retrieve_appointments(dealer_integration_partner_id, first_name, last_name, email_address, phone_number, vin)

        retrieve_appts_arn = loads(partner_metadata).get("retrieve_appts_arn", "")

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

        response = invoke_vendor_lambda(payload, retrieve_appts_arn)
        if response["statusCode"] == 500:
            logger.error(f"Integration encountered error: {response}")
            body = loads(response["body"])
            return {
                "statusCode": "500",
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
        body = loads(response["body"])
        vendor_appointments = body["appointments"]

        appointments = []
        for db_appt in db_appointments:
            for v_appt in vendor_appointments:
                # Appointment found in vendor
                if db_appt["integration_appointment_id"] == v_appt["appointment_id"]:
                    if v_appt.get("status") and db_appt["status"] != v_appt["status"]:
                        # Update appointment in db
                        db_appt["status"] = v_appt["status"]
                        rds_instance.update_appointment(db_appt)
                        appointments.append(db_appt)
                    break
            else:
                # Appointment not found in vendor
                current_time = datetime.utcnow()
                timeslot_time = datetime.strptime(db_appt["timeslot"], "%Y-%m-%dT%H:%M:%S")
                if timeslot_time < current_time:
                    logger.info(f"Appointment {db_appt['id']} timeslot has passed.")
                    db_appt["status"] = "Completed"
                else:
                    logger.info(f"Appointment {db_appt['id']} not found in vendor. Assumed Lost.")
                    db_appt["status"] = "Lost"
                    rds_instance.update_appointment(db_appt)
                appointments.append(db_appt)

        # Add appointments from vendor which weren't in db
        for appt in vendor_appointments:
            for db_appt in db_appointments:
                if appt["appointment_id"] == db_appt["integration_appointment_id"]:
                    break
            else:
                product_op_code = rds_instance.get_product_op_code(dealer_integration_partner_id, appt["op_code"], product_id)
                appointment = {
                    "op_code": product_op_code if product_op_code else None,
                    "timeslot": appt["timeslot"],
                    "timeslot_duration": appt["duration"],
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

        for appt in vendor_appointments:
            for db_appt in db_appointments:
                if appt["appointment_id"] == db_appt["id"]:
                    # Update appointment in db
                    # appointment = update_appointment(appt, db_appt)
                    # appointments.append(appointment)
                    break
            else:
                product_op_code = rds_instance.get_product_op_code(dealer_integration_partner_id, appt["op_code"], product_id)
                appointment = {
                    "op_code": product_op_code,
                    "timeslot": appt["timeslot"],
                    "timeslot_duration": appt["duration"],
                    "comment": appt.get("comment"),
                    "status": "Active",
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

        return {
            "statusCode": "200",
            "body": dumps({
                "appointments": appointments,
                "request_id": request_id,
            })
        }

    except IntegrationError as e:
        logger.error(f"Integration error: {e}")
        return {
            "statusCode": "500",
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
        return {
            "statusCode": "500",
            "body": dumps({
                "error": str(e),
                "request_id": request_id,
            })
        }
