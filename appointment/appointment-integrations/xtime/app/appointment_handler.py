"""Lambda handlers for XTime API integration."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import dumps
from logging import getLogger
from os import environ
from typing import Any

from ratelimit import limits, sleep_and_retry

from models import GetAppointments, CreateAppointment, AppointmentSlots
from utils import parse_event, validate_data, handle_exception, format_and_filter_timeslots
from xtime_api_wrapper import XTimeApiWrapper

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_appt_time_slots(event: Any, context: Any) -> Any:
    """Get available appointment time slots from XTime."""
    logger.info(f"Event: {event}")

    try:
        data = parse_event(event)
        appointment_slots = validate_data(data, AppointmentSlots)

        api_wrapper = XTimeApiWrapper()
        appt_time_slots = api_wrapper.retrieve_appt_time_slots(
            appointment_slots=appointment_slots
        )

        if appt_time_slots["success"]:
            timeslots = format_and_filter_timeslots(appt_time_slots["availableAppointments"],
                                                    start_time=appointment_slots.start_time,
                                                    end_time=appointment_slots.end_time)
            return {
                "statusCode": 200,
                "body": dumps(
                    {
                        "available_timeslots": timeslots
                    }
                )
            }

        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "error": {
                        "code": "V002",
                        "message": "XTime responded with an error: {} {}".format(
                            appt_time_slots["code"], appt_time_slots["message"]
                        ),
                    }
                }
            ),
        }

    except Exception as e:
        return handle_exception(e, "get_appt_time_slot")


def create_appointment(event: Any, context: Any) -> Any:
    """Create an appointment on XTime."""
    logger.info(f"Event: {event}")

    try:
        data = parse_event(event)
        create_appointment_data = validate_data(data, CreateAppointment)

        api_wrapper = XTimeApiWrapper()
        create_appointment = api_wrapper.create_appointments(create_appointment_data)

        if create_appointment["success"]:
            return {
                "statusCode": 201,
                "body": dumps(
                    {
                        "appointment_id": create_appointment["appointmentId"]
                    }
                ),
            }

        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "error": {
                        "code": "V002",
                        "message": "XTime responded with an error: {} {}".format(
                            create_appointment["code"], create_appointment["message"]
                        ),
                    }
                }
            ),
        }

    except Exception as e:
        return handle_exception(e, "create_appointment")


def get_appointments(event: Any, context: Any) -> Any:
    """Get appointments from XTime."""
    logger.info(f"Event: {event}")

    try:
        data = parse_event(event)
        get_appointments_data = validate_data(data, GetAppointments)

        api_wrapper = XTimeApiWrapper()
        appointments_data = api_wrapper.retrieve_appointments(get_appointments_data)

        if appointments_data["success"]:
            appointments = [
                {
                    "appointment_id": appt.get("appointmentId"),
                    "vin": appt.get("vin"),
                    "timeslot": appt.get("appointmentDateTimeLocal"),
                    # "timeslot_duration": 15,  # It's default for XTime
                    "comment": appt.get("comment"),
                    "first_name": appt.get("firstName"),
                    "last_name": appt.get("lastName"),
                    "email_address": appt.get("emailAddress"),
                    "phone_number": appt.get("phoneNumber"),
                    "services": [
                        {
                            "op_code": service.get("opcode"),
                            "service_name": service.get("serviceName"),
                        }
                        for service in appt["services"]
                    ],
                }
                for appt in appointments_data["appointments"]
            ]
            return {
                "statusCode": 200,
                "body": dumps({
                    "appointments": appointments
                })
            }

        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": "V002",
                    "message": "XTime responded with an error: {} {}".format(
                        appointments_data["code"], appointments_data["message"]
                    ),
                }
            })
        }

    except Exception as e:
        return handle_exception(e, "get_appointments")


@sleep_and_retry
@limits(calls=10, period=1)
def fetch_codes_from_xtime(api_wrapper, integration_dealer_id):
    """
    Helper to fetch dealer codes for a single dealer from XTime and handles errors.
    """

    try:
        response = api_wrapper.get_dealer_codes(integration_dealer_id)

        if response["success"]:
            opcodes = [service.get("opcode") for service in response.get("services", [])]
            return integration_dealer_id, opcodes
        else:
            logger.error(f"XTime error for dealer {integration_dealer_id}: {response}")
            return integration_dealer_id, "ERROR"
    except Exception as e:
        logger.error(f"Error fetching dealer {integration_dealer_id}: {e}")
        return integration_dealer_id, "ERROR"


def get_dealer_codes(event, context):
    """Get standard dealer opcodes from XTime."""
    logger.info(f"Event: {event}")

    try:
        api_wrapper = XTimeApiWrapper()

        data = parse_event(event)
        integration_dealer_ids = data["integration_dealer_ids"]

        dealer_codes = {}
        with ThreadPoolExecutor() as executor:
            future_to_id = {
                executor.submit(fetch_codes_from_xtime, api_wrapper, dealer_id): dealer_id
                for dealer_id in integration_dealer_ids
            }

            for future in as_completed(future_to_id):
                dealer_id = future_to_id[future]
                try:
                    integration_dealer_id, opcodes = future.result()
                    dealer_codes[integration_dealer_id] = opcodes
                except Exception as e:
                    logger.error(f"Unhandled exception for dealer {dealer_id}: {e}")
                    dealer_codes[dealer_id] = "ERROR"

        return {
            "statusCode": 200,
            "body": dumps({
                "dealer_codes": dealer_codes
            })
        }
    except Exception as e:
        logger.exception(f"Error in get_dealer_codes: {e}")
        return {
            "statusCode": 500,
            "body": dumps({
                "error": "XTime responded with an error: {}".format(e)
            })
        }
