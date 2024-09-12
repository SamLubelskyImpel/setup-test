import boto3
import logging
from os import environ
from json import loads, dumps
from typing import Any, Dict
from uuid import uuid4
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

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

def upload_appointment_to_s3(appointment: Dict[str, Any], dms_id: str, source_s3_uri: str, index: int) -> Any:
    """Upload appointment to s3 database."""
    format_string = 'PartitionYear=%Y/PartitionMonth=%m/PartitionDate=%d'
    date_key = datetime.utcnow().strftime(format_string)
    original_file = source_s3_uri.split("/")[-1].split(".")[0]
    logger.info(f"Appointment data to send: {appointment}")

    s3_key = f"unified/service_appointment/quiter/dealer_integration_partner|dms_id={dms_id}/{date_key}/{original_file}_{str(index)}_{str(uuid4())}.json"
    logger.info(f"Saving leads to {s3_key}")
    S3_CLIENT.put_object(
        Body=dumps(appointment),
        Bucket=BUCKET,
        Key=s3_key,
    )
    
def parse_json_to_entries(dms_id: int, json_data: dict) -> Any:
    db_dealer_integration_partner = {"dms_id": dms_id}
    entries = []

    for record in json_data:
        db_service_appointment = {
            "appointment_time":record.get("Appointment Time", None),
            "appointment_date":record.get("Appointment Date", None),
            "appointment_source":record.get("Appointment Source", None),
            "reason_code":record.get("Reason Code", None),
            "appointment_create_ts":record.get("Appointment Create TS", None),
            "appointment_update_ts":record.get("Appointment Update TS", None),
            "rescheduled_flag":record.get("Rescheduled Flag", None),
            "appointment_no":record.get("Appointment No", None),
            "last_ro_date":record.get("Last RO Date", None),
            "last_ro_num"record.get("Last RO No", None):
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
            "warranty_expiration_date":record.get("Warranty Expiration Date", None)
        }
        db_consumer = {
            "dealer_customer_no":record.get("Consumer ID", None),
            "first_name":record.get("First Name", None),
            "last_name":record.get("Last Name", None),
            "email":record.get("Email", None),
            "cell_phone":record.get("Cell Phone", None)
            "city":record.get("City", None),
            "state":record.get("State", None),
            "metro":record.get("Metro", None),
            "postal_code":record.get("Postal Code", None),
            "email_optin_flag":record.get("Email Optin Flag", None),
            "phone_optin_flag":record.get("Phone Optin Flag", None),
            "postal_mail_optin_flag":record.get("Postal Mail Optin Flag", None),
            "sms_optin_flag":record.get("SMS Optin Flag", None),
            "master_consumer_id":record.get("Master Consumer Id", None)
        }

        # No Mappings for Service contracts
        db_service_contracts = []
        # Op Codes mappings are foreign keys?
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

   return entries

def post_entry(entry: dict, index: int) -> bool:
    """Process a single entry."""
    logger.info(f"[THREAD {index}] Processing entry {entry}")
    try:
        # lead = entry["lead"]
        unified_crm_lead_id = upload_appointment_to_s3(entry, index)
        logger.info(f"[THREAD {index}] Appointment successfully updated: {unified_crm_lead_id}")
    except Exception as e:
        if '409' in str(e):
            # Log the 409 error and continue with the next entry
            logger.warning(f"[THREAD {index}] {e}")
        else:
            logger.error(f"[THREAD {index}] Error uploading entry to S3: {e}")
            return False

    return True

def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        bucket = os.environ["INTEGRATIONS_BUCKET"]
        key = message["s3_key"]
        dms_id = message["dms_id"]

        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        content = pd.read_csv(response["Body"])
        json_data = content.to_json(orient="records")
        logger.info(f"Raw data: {json_data}")

        entries = parse_json_to_entries(dms_id, json_data)
        logger.info(f"Transformed entries: {entries}")


        results = []
        # Process each entry in parallel, each entry takes about 8 seconds to process.
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(post_entry,
                                entry, dms_id, key, idx)
                for idx, entry in enumerate(entries)
            ]
            for future in as_completed(futures):
                results.append(future.result())

        for result in results:
            if not result:
                raise Exception("Error detected posting and forwarding an entry")

    except Exception as e:
        logger.error(f"Error transforming quiter appointment record - {record}: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw pbs data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise