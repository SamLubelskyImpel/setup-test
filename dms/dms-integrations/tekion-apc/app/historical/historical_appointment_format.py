"""Format tekion historical csv data to unified format."""
import logging
import urllib.parse
from json import dumps, loads
from os import environ
import csv
import boto3
from datetime import datetime
from unified_df import upload_unified_json

REGION = environ.get("REGION", "us-east-1")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def convert_unix_to_timestamp(unix_time):
    """Convert unix time to datetime object"""
    try:
        unix_time = int(unix_time)
        if unix_time == 0:
            return None
    except ValueError:
        return None

    return datetime.utcfromtimestamp(unix_time / 1000).strftime("%Y-%m-%d %H:%M:%S")


def parse_csv_to_entries(csv_data, s3_uri):
    entries = []
    dms_id = s3_uri.split('/')[3]
    reader = csv.DictReader(csv_data.splitlines())

    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[4],
        "PartitionMonth": s3_uri.split("/")[5],
        "PartitionDate": s3_uri.split("/")[6],
        "s3_url": s3_uri,
    }

    for row in reader:
        # Convert all keys to lowercase
        normalized_row = {k.lower(): v for k, v in row.items()}

        db_service_appointment = {}
        db_vehicle = {}
        db_consumer = {}

        db_dealer_integration_partner = {
            'dms_id': dms_id
        }

        db_service_appointment["appointment_no"] = normalized_row["appointmentid"]

        appointment_time = normalized_row.get("appttime")
        converted_appt_time = convert_unix_to_timestamp(appointment_time)
        if converted_appt_time:
            db_service_appointment["appointment_date"], db_service_appointment["appointment_time"] = converted_appt_time.split(" ")

        db_service_appointment["appointment_create_ts"] = convert_unix_to_timestamp(normalized_row.get("appointmentcreatedtime"))
        db_service_appointment["appointment_update_ts"] = convert_unix_to_timestamp(normalized_row.get("appointmentmodifiedtime"))

        db_service_appointment["appointment_source"] = normalized_row.get("source")
        db_service_appointment["last_ro_num"] = normalized_row.get("rono")

        db_consumer["dealer_customer_no"] = normalized_row.get("custinfo_id")
        db_consumer["first_name"] = normalized_row.get("custinfo_firstname")
        db_consumer["last_name"] = normalized_row.get("custinfo_lastname")
        db_consumer["email"] = normalized_row.get("custinfo_email")
        db_consumer["cell_phone"] = normalized_row.get("custinfo_mobilenumber")
        db_consumer["home_phone"] = normalized_row.get("custinfo_homephone")
        db_consumer["email_optin_flag"] = bool(normalized_row.get("email"))
        db_consumer["postal_mail_optin_flag"] = normalized_row["optmail"].lower() == 'true' if normalized_row.get("optmail") else False
        db_consumer["sms_optin_flag"] = normalized_row["opttxt"].lower() == 'true' if normalized_row.get("opttxt") else False

        db_vehicle["vin"] = normalized_row.get("vehinfo_vin")
        db_vehicle["oem_name"] = normalized_row.get("vehinfo_make")
        db_vehicle["make"] = normalized_row.get("vehinfo_make")
        db_vehicle["model"] = normalized_row.get("vehinfo_model")
        db_vehicle["year"] = int(float(normalized_row["vehinfo_year"])) if normalized_row.get("vehinfo_year") and normalized_row["vehinfo_year"].isdigit() else None
        db_vehicle["type"] = normalized_row.get("vehinfo_bodytype")
        db_vehicle["vehicle_class"] = normalized_row.get("vehinfo_bodytype")

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_service_appointment["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "appointment": db_service_appointment,
            "vehicle": db_vehicle,
            "consumer": db_consumer
        }
        entries.append(entry)

    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion historical appointment files."""
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
                csv_data = response["Body"].read().decode('utf-8')
                entries, dms_id = parse_csv_to_entries(csv_data, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "service_appointment", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion historical appointment file {event}")
        raise
