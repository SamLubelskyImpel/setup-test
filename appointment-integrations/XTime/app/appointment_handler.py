import logging
from os import environ
from typing import Any
from json import dumps, loads
from api_wrapper import XTimeApiWrapper
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

        appt_time_slots = api_wrapper.retrieve_appt_time_slots(appointment_slots=appointment_slots)
        
        if appt_time_slots["success"]:
            return {
                "statusCode": 200,
                "body": {
                    "available_timeslots": [
                        {
                            "timeslot": time_slot["appointmentDateTimeLocal"],
                            "duration": time_slot["durationMinutes"]
                        } for time_slot in appt_time_slots["availableAppointments"]
                    ]
                }
            }

        return {
            "statusCode": 401,
            "body": dumps({"message": f"Response from XTime: {appt_time_slots["message"]}."}),
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
                "body": {
                    "appointment_id": create_appointment["appointmentId"]
                }
            }
        
        return {
            "statusCode": 401,
            "body": dumps({"message": f"Response from XTime: {create_appointment["message"]}."}),
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
                "body": {
                    "appointments": [
                        {
                            "appointment_id": appt["appointmentId"],
                            "vin": appt["vin"],
                            "timeslot": appt["appointmentDateTimeLocal"],
                            "timeslot_duration": appt["appointmentDateTimeLocal"],
                            "comment": appt["comment"],
                            "first_name": appt["firstName"],
                            "last_name": appt["lastName"],
                            "email_address": appt["emailAddress"],
                            "phone_number": appt["phoneNumber"],
                            "op_code": appt["services"][0]["opcode"]
                        } for appt in appointments_data["appointments"]
                    ]
                }
            }
        
        return {
            "statusCode": 401,
            "body": dumps({"message": f"Response from XTime: {appointments_data["message"]}."}),
        }


    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
