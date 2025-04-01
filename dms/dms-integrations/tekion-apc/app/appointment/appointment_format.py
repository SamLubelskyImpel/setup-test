"""Format tekion json data to unified format."""
import logging
import urllib.parse
from datetime import datetime
from json import dumps, loads
from os import environ
import boto3
from unified_df import upload_unified_json

REGION = environ.get("REGION", "us-east-1")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def default_get(json_dict, key, default_value=None):
    """Return a default value if a key doesn't exist or if it's value is None."""
    json_value = json_dict.get(key)
    return json_value if json_value is not None else default_value


def convert_unix_to_timestamp(unix_time):
    """Convert unix time to datetime object"""
    if not unix_time or not isinstance(unix_time, int) or unix_time == 0:
        return None
    return datetime.utcfromtimestamp(unix_time / 1000)


def parse_json_to_entries(json_data, s3_uri):
    """Format tekion json data to unified format."""
    entries = []

    dms_id = None
    for appointment in json_data:
        db_dealer_integration_partner = {}
        db_service_appointment = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        db_metadata = {
            "Region": REGION,
            "PartitionYear": s3_uri.split("/")[2],
            "PartitionMonth": s3_uri.split("/")[3],
            "PartitionDate": s3_uri.split("/")[4],
            "s3_url": s3_uri,
        }

        dms_id = default_get(appointment, "dms_id")  # Added to payload from parent lambda
        db_dealer_integration_partner["dms_id"] = dms_id

        db_service_appointment["appointment_no"] = default_get(
            appointment, "appointmentNumber"
        )
        appointment_time = default_get(appointment, "appointmentDateTime")
        converted_appt_time = convert_unix_to_timestamp(appointment_time)
        if converted_appt_time:
            db_service_appointment["appointment_time"] = converted_appt_time.strftime("%H:%M:%S")
            db_service_appointment["appointment_date"] = converted_appt_time.strftime("%Y-%m-%d")

        vehicle = default_get(appointment, "vehicle", {})
        converted_ro_date = convert_unix_to_timestamp(default_get(vehicle, "lastServiceDate"))
        if converted_ro_date:
            db_service_appointment["last_ro_date"] = converted_ro_date.strftime("%Y-%m-%d")

        db_vehicle["vin"] = default_get(vehicle, "vin")
        db_vehicle["oem_name"] = default_get(vehicle, "make")
        db_vehicle["make"] = default_get(vehicle, "make")
        db_vehicle["model"] = default_get(vehicle, "model")
        year_field = default_get(vehicle, "year")
        year = int(year_field) if year_field and year_field.isdigit() else None
        db_vehicle["year"] = year
        mileage_field = default_get(vehicle, "mileage")
        mileage = int(mileage_field) if mileage_field else None
        db_vehicle["mileage"] = mileage
        db_vehicle["stock_num"] = default_get(vehicle, "stockNumber")
        db_vehicle["trim"] = default_get(vehicle, "trim")
        db_service_appointment["customer_comments"] = default_get(appointment, "customerComments")

        if mileage and year:
            current_year = datetime.now().year
            if mileage < 3000 and year >= current_year-1:
                db_vehicle["new_or_used"] = "N"
            else:
                db_vehicle["new_or_used"] = "U"
        else:
            db_vehicle["new_or_used"] = None

        customer = default_get(appointment, "customer", {})
        db_consumer["dealer_customer_no"] = default_get(customer, "arcId")
        db_consumer["first_name"] = default_get(customer, "firstName")
        db_consumer["last_name"] = default_get(customer, "lastName")
        db_consumer["email"] = default_get(customer, "email")

        phones = default_get(customer, "phones", [])
        for phone in phones:
            phone_type = default_get(phone, "phoneType", "")
            phone_number = default_get(phone, "number")
            if phone_type.upper() == "MOBILE":
                db_consumer["cell_phone"] = phone_number
            elif phone_type.upper() == "HOME":
                db_consumer["home_phone"] = phone_number

        address = default_get(customer, "address", {})
        db_consumer["address"] = default_get(address, "line1")
        db_consumer["city"] = default_get(address, "city")
        db_consumer["state"] = default_get(address, "state")
        db_consumer["postal_code"] = default_get(address, "zipCode")

        jobs = default_get(appointment, "jobs", [])
        for job in jobs:
            operations = default_get(job, "operations", [])
            for operation in operations:
                db_op_code = {}
                db_op_code["op_code|op_code"] = default_get(operation, "opcode")
                db_op_code["op_code|op_code_desc"] = (default_get(operation, "opcodeDescription") or "")[:305]
                db_op_codes.append(db_op_code)

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_service_appointment["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "appointment": db_service_appointment,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "op_codes.op_codes": db_op_codes
        }
        entries.append(entry)
    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion appointment files."""
    try:
        logger.info(event)
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                json_data = loads(response["Body"].read())
                entries, dms_id = parse_json_to_entries(json_data, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "service_appointment", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion appointment file {event}")
        raise
