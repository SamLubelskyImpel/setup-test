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

sm_client = boto3.client('secretsmanager')
s3_client = boto3.client("s3")


def get_secret(secret_name, secret_key) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
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
    s3_client.put_object(
        Body=dumps(appointment),
        Bucket=BUCKET,
        Key=s3_key,
    )
    
def parse_json_to_entries(dms_id: int, json_data: dict) -> Any:

   return

def post_entry(entry: dict, index: int) -> bool:
    """Process a single entry."""
    logger.info(f"[THREAD {index}] Processing entry {entry}")
    try:
        lead = entry["lead"]
        unified_crm_lead_id = upload_appointment_to_s3(lead, index)
        logger.info(f"[THREAD {index}] Appointment successfully updated: {unified_crm_lead_id}")
    except Exception as e:
        if '409' in str(e):
            # Log the 409 error and continue with the next entry
            logger.warning(f"[THREAD {index}] {e}")
        else:
            logger.error(f"[THREAD {index}] Error uploading entry to S3: {e}")
            return False

    return True

#TODO: figure out how to work with csv instead of json
def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]
        dms_id = key.split('/')[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        json_data = loads(content)
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