import boto3
import logging
import pandas as pd
import urllib.parse
import io
from os import environ
from json import loads
from typing import Any, Dict
from datetime import datetime
from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = environ.get("CLIENT_ENGINEERING_SNS_TOPIC_ARN")

SM_CLIENT = boto3.client('secretsmanager')
S3_CLIENT = boto3.client("s3")

def get_secret(secret_name, secret_key) -> Any:
    """Get secret from Secrets Manager."""
    secret = SM_CLIENT.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data

def convert_to_timestamp(date_str):
    """Convert a date string in 'day/month/year' format to a datetime object"""
    try:
        # Convert the string '13/05/24' to 'YYYY-MM-DD' format
        return datetime.strptime(date_str, "%d/%m/%y").strftime("%Y-%m-%d 00:00:00.000")
    except ValueError:
        logger.error(f"Error parsing date: {date_str}")
        return None
    
def parse_json_to_entries(json_data: dict) -> Any:

    entries = []

    for record in json_data:
        dms_id = record.get("Dealer ID")
        db_dealer_integration_partner = {"dms_id": dms_id}

        # Convert dates to US date format
        appointment_date = convert_to_timestamp(record.get("Appointment Date"))
        appointment_create_ts = convert_to_timestamp(record.get("Appointment Create TS"))
        appointment_update_ts = convert_to_timestamp(record.get("Appointment Update TS"))
        ro_date = record.get("Last RO Date", None)
        last_ro_date = None
        if ro_date:
            last_ro_date = convert_to_timestamp(ro_date)

        warranty_date = record.get("Warranty Expiration Date", None)
        warranty_expiration_date = None
        if warranty_date:
            warranty_expiration_date = datetime.strptime(warranty_date , "%d/%m/%y").strftime("%m/%d/%y")

        # Handle truncated fields
        appointment_source = record.get("Appointment Source")[:100] if record.get("Appointment Source", None) else None
        reason_code = record.get("Reason Code")[:100] if record.get("Reason Code", None) else None
        metro = record.get("Metro")[:80] if record.get("Metro", None) else None
        city = record.get("City")[:80] if record.get("City", None) else None

        db_service_appointment = {
            "appointment_time":record.get("Appointment Time", None),
            "appointment_date":appointment_date,
            "appointment_source":appointment_source,
            "reason_code":reason_code,
            "appointment_create_ts":appointment_create_ts,
            "appointment_update_ts":appointment_update_ts,
            "rescheduled_flag":record.get("Rescheduled Flag", None),
            "appointment_no":record.get("Appointment No", None),
            "last_ro_date":last_ro_date,
            "last_ro_num": record.get("Last RO No", None)
        }
        db_vehicle = {
            "vin":record.get("Vin No", None),
            "oem_name":record.get("OEM Name", None),
            "type":record.get("Vehicle Type", None),
            "vehicle_class":record.get("Vehicle Class", None),
            "mileage":record.get("Mileage on Vehicle", None),
            "make":record.get("Make", None),
            "model":record.get("Model", None),
            "year":record.get("Year", None),
            "new_or_used":record.get("New or Used", None),
            "warranty_expiration_miles":record.get("Warranty Expiration Miles", None),
            "warranty_expiration_date":warranty_expiration_date
        }
        db_consumer = {
            "dealer_customer_no":record.get("Consumer ID", None),
            "first_name":record.get("First Name", None),
            "last_name":record.get("Last Name", None),
            "email":record.get("Email", None),
            "cell_phone":record.get("Cell Phone", None),
            "city":city,
            "state":record.get("State", None),
            "metro":metro,
            "postal_code":record.get("Postal Code", None),
            "email_optin_flag":record.get("Email Optin Flag", None),
            "phone_optin_flag":record.get("Phone Optin Flag", None),
            "postal_mail_optin_flag":record.get("Postal Mail Optin Flag", None),
            "sms_optin_flag":record.get("SMS Optin Flag", None),
            "master_consumer_id":record.get("Master Consumer Id", None)
        }

        # No Mappings for Service contracts
        db_service_contracts = []
        # Op Codes mappings are all foreign keys?
        db_op_codes = []

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "appointment": db_service_appointment,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "service_contracts.service_contracts": db_service_contracts,
            "op_codes.op_codes": db_op_codes,
        }
        entries.append(entry)

    return entries, dms_id

def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw quiter data to the unified format."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            
            # Process each S3 record in the event
            for s3_record in message.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                
                # Fetch the object from S3
                response = S3_CLIENT.get_object(Bucket=bucket, Key=decoded_key)
                csv_data = response["Body"].read().decode("utf-8") 

                # Use pandas to read the CSV content into a DataFrame
                csv_df = pd.read_csv(io.StringIO(csv_data),dtype={'Dealer ID': 'string'})
                json_data = loads(csv_df.to_json(orient="records"))
                # logger.info(f"Raw Data:{json_data}")

                # Process the CSV entries using the modified function
                entries, dms_id = parse_json_to_entries(json_data)

                if not dms_id:
                    raise RuntimeError("No dms_id found in the CSV data")
                # Call the upload function with the parsed entries
                upload_unified_json(entries, "service_appointment", decoded_key, dms_id)
                
    except Exception as e:
        logger.exception(f"Error transforming vehicle sale file {event}: {e}")
        raise

def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="QuiterFormatInsertFilesAppointment Lambda Error",
        Message=str(error_message),
    )
    return