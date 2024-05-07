from os import environ
from typing import Any
from logging import getLogger
from xtime_api_wrapper import XTimeApiWrapper
from models import GetAppointments, CreateAppointment, AppointmentSlots
from datetime import datetime

from utils import parse_event, validate_data, handle_exception, lambda_response

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_appt_time_slots(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        data = parse_event(event)
        appointment_slots = validate_data(data, AppointmentSlots)

        api_wrapper = XTimeApiWrapper()

        appt_time_slots = api_wrapper.retrieve_appt_time_slots(
            appointment_slots=appointment_slots
        )

        logger.info(appt_time_slots["availableAppointments"])

        if appt_time_slots["success"]:

            def formatted_time(time_string: str) -> str:
                dt = datetime.fromisoformat(time_string.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%dT%H:%M')

            return lambda_response(
                200,
                {
                    "available_timeslots": [
                        {
                            "timeslot": formatted_time(time_slot["appointmentDateTimeLocal"]),
                            "duration": time_slot["durationMinutes"],
                        }
                        for time_slot in appt_time_slots["availableAppointments"]
                    ]
                },
            )
        return lambda_response(
            500,
            {
                "error": {
                    "code": appt_time_slots.get("code", "401"),
                    "message": appt_time_slots["message"],
                }
            },
        )

    except Exception as e:
        return handle_exception(e, "get_appt_time_slot")


def create_appointment(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        data = parse_event(event)
        create_appointment_data = validate_data(data, CreateAppointment)

        api_wrapper = XTimeApiWrapper()

        create_appointment = api_wrapper.create_appointments(create_appointment_data)

        if create_appointment["success"]:
            return lambda_response(
                201, {"appointment_id": create_appointment["appointmentId"]}
            )

        return lambda_response(
            500,
            {
                "error": {
                    "code": create_appointment["code"],
                    "message": create_appointment["message"],
                }
            },
        )

    except Exception as e:
        return handle_exception(e, "create_appointment")


def get_appointments(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        data = parse_event(event)
        get_appointments_data = validate_data(data, GetAppointments)

        api_wrapper = XTimeApiWrapper()

        appointments_data = api_wrapper.retrieve_appointments(get_appointments_data)

        if appointments_data["success"]:
            return lambda_response(
                200,
                {
                    "appointments": [
                        {
                            "appointment_id": appt.get("appointmentId"),
                            "vin": appt.get("vin"),
                            "timeslot": appt.get("appointmentDateTimeLocal"),
                            "timeslot_duration": 15,  # It's default for XTime
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
                },
            )

        return lambda_response(
            500,
            {
                "error": {
                    "code": appointments_data["code"],
                    "message": appointments_data["message"],
                }
            },
        )
    except Exception as e:
        return handle_exception(e, "get_appointments")
