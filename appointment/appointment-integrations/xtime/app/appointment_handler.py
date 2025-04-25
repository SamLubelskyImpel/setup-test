"""Lambda handlers for XTime API integration."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import dumps
from logging import getLogger
from os import environ
from typing import Any

from models import GetAppointments, CreateAppointment, AppointmentSlots, UpdateAppointment
from utils import parse_event, validate_data, handle_response, format_and_filter_timeslots, send_alert_notification
from xtime_api_wrapper import XTimeApiWrapper
from uuid import uuid4

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

DEFAULT_ERROR_MSG = "Vendor integration had an unexpected error. Please contact Impel support."
                      
def get_appt_time_slots(event: Any, context: Any) -> Any:
    """Get available appointment time slots from XTime."""
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
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

            body = {"available_timeslots": timeslots}
            return handle_response(request_id, "get_appt_time_slot", 200, body)

        error_msg = f"XTime responded with an error: {appt_time_slots['code']} - {appt_time_slots['message']}"
        body = {"error": {"code": "V002", "message": error_msg}}
        
        return handle_response(request_id, "get_appt_time_slot", 500, body)

    except Exception as e:
        body = {"error": {"code": "V002", "message": DEFAULT_ERROR_MSG}}
        return handle_response(request_id, "get_appt_time_slot", 500, body, e)


def create_appointment(event: Any, context: Any) -> Any:
    """Create an appointment on XTime."""
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info(f"Event: {event}")

    try:
        data = parse_event(event)
        create_appointment_data = validate_data(data, CreateAppointment)

        api_wrapper = XTimeApiWrapper()
        create_appointment = api_wrapper.create_appointments(create_appointment_data)

        if create_appointment["success"]:
            body = {"appointment_id": create_appointment["appointmentId"]}
            return handle_response(request_id, "create_appointment", 201, body)

        error_msg = f"XTime responded with an error: {create_appointment['code']} - {create_appointment['message']}"
        body = {"error": {"code": "V002", "message": error_msg}}

        return handle_response(request_id, "create_appointment", 500, body)
    

    except Exception as e:
        body = {"error": {"code": "V002", "message": DEFAULT_ERROR_MSG}}
        return handle_response(request_id, "create_appointment", 500, body, e)


def get_appointments(event: Any, context: Any) -> Any:
    """Get appointments from XTime."""
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
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

            body = {"appointments": appointments}
            return handle_response(request_id, "get_appointments", 200, body)

        error_msg = f"XTime responded with an error: {appointments_data['code']} - {appointments_data['message']}"
        body = {"error": {"code": "V002", "message": error_msg}}

        return handle_response(request_id, "get_appointments", 500, body)

    except Exception as e:
        body = {"error": {"code": "V002", "message": DEFAULT_ERROR_MSG}}
        return handle_response(request_id, "get_appointments", 500, body, e)


def update_appointment(event: Any, context: Any) -> Any:
    """Update an appointment in XTime."""
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info(f"Event: {event}")

    try:
        data = parse_event(event)
        api_wrapper = XTimeApiWrapper()

        update_appointment_data = validate_data(data, UpdateAppointment)
        update_appointment = api_wrapper.update_appointment(update_appointment_data)

        if update_appointment["success"]:
            body = {"message": "Appointment successfully rescheduled"}
            return handle_response(request_id, "update_appointment", 200, body)

        error_msg = f"XTime responded with an error: {update_appointment['code']} - {update_appointment['message']}"
        body = {"error": {"code": "V002", "message": error_msg}}

        return handle_response(request_id, "update_appointment", 500, body)

    except Exception as e:
        body = {"error": {"code": "V002", "message": DEFAULT_ERROR_MSG}}
        return handle_response(request_id, "update_appointment", 500, body, e)


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
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
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

        body = {"dealer_codes": dealer_codes}
        return handle_response(request_id, "get_dealer_codes", 200, body)

    except Exception as e:
        error_msg = f"XTime responded with an error: {e}"
        body = {"error": {"code": "V002", "message": error_msg}}
        return handle_response(request_id, "get_dealer_codes", 500, body, e)