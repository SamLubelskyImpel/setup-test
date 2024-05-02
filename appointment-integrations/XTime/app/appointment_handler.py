import logging
from os import environ
from typing import Any
from json import dumps, loads
from xtime_api_wrapper import XTimeApiWrapper
from models import GetAppointments, CreateAppointment, AppointmentSlots

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_appt_time_slots(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        if event.get("body"):
            body = loads(event["body"])
            appointment_slots = AppointmentSlots(**body)
        else:
            appointment_slots = AppointmentSlots(**event)

        api_wrapper = XTimeApiWrapper()

        appt_time_slots = api_wrapper.retrieve_appt_time_slots(
            appointment_slots=appointment_slots
        )

        logger.info(appt_time_slots["availableAppointments"])

        if appt_time_slots["success"]:
            return {
                "statusCode": 200,
                "body": dumps({
                    "available_timeslots": [
                        {
                            "timeslot": time_slot["appointmentDateTimeLocal"],
                            "duration": time_slot["durationMinutes"],
                        }
                        for time_slot in appt_time_slots["availableAppointments"]
                    ]
                })
            }

        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "error": {
                        "code": appt_time_slots["code"],
                        "message": appt_time_slots["message"],
                    }
                }
            ),
        }
    
    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }


def create_appointment(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        if event.get("body"):
            body = loads(event["body"])
            create_appointment_data = CreateAppointment(**body)
        else:
            create_appointment_data = CreateAppointment(**event)

        api_wrapper = XTimeApiWrapper()

        create_appointment = api_wrapper.create_appointments(create_appointment_data)

        if create_appointment["success"]:
            return {
                "statusCode": 201,
                "body": {"appointment_id": create_appointment["appointmentId"]},
            }

        return {
            "statusCode": 401,
            "body": dumps(
                {"message": f"Response from XTime: {create_appointment['message']}."}
            ),
        }

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }


def get_appointments(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        if event.get("body"):
            body = loads(event["body"])
            get_appointments_data = GetAppointments(**body)
        else:
            get_appointments_data = GetAppointments(**event)

        api_wrapper = XTimeApiWrapper()

        appointments_data = api_wrapper.retrieve_appointments(get_appointments_data)

        if appointments_data["success"]:
            return {
                "statusCode": 200,
                "body": dumps({
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
                },)
            }

        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "error": {
                        "code": appointments_data["code"],
                        "message": appointments_data["message"],
                    }
                }
            ),
        }

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": f"An error occurred while processing the request.\n{e}"}),
        }
