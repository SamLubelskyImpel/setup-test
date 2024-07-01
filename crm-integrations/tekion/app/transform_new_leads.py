"""Transform raw tekion crm data to the unified format."""

import boto3
import logging
import requests
from os import environ
from json import loads
from typing import Any, Dict
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
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
DA_SECRET_KEY = environ.get("DA_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

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


def upload_to_db(data: Dict[str, Any], endpoint: str, api_key: str, index: int, id_key: str) -> Any:
    """Upload data to the database through CRM API."""
    logger.info(f"Data to send: {data}")
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/{endpoint}",
        json=data,
        headers={
            "x_api_key": api_key,
            "partner_id": UPLOAD_SECRET_KEY,
        },
    )
    logger.info(
        f"[THREAD {index}] Response from Unified Layer Create {endpoint.capitalize()} {response.status_code} {response.text}",
    )
    response.raise_for_status()
    unified_crm_id = response.json().get(id_key)

    if not unified_crm_id:
        logger.error(f"Error creating {endpoint}: {data}")
        raise Exception(f"Error creating {endpoint}: {data}")

    return unified_crm_id


def format_ts(input_ts: str) -> str:
    """
    Format a timestamp string into a db format.

    Assumes the input timestamp is either:
    - A timestamp string in milliseconds since the epoch, e.g., "1719257050849"
    - An empty string, in which case the current datetime will be used.
    """
    if not input_ts:
        dt = datetime.utcnow()
    else:
        # Assume the input is a timestamp in milliseconds
        dt = datetime.utcfromtimestamp(int(input_ts) / 1000)

    output_format = "%Y-%m-%dT%H:%M:%SZ"
    return dt.strftime(output_format)


def parse_email(emails):
    """Parse the emails."""
    if not emails:
        return None

    preferred_email = emails[0]

    for email in emails:
        if email.get('isPrimary') is True:
            preferred_email = email
            break

    return preferred_email


def parse_phone_number(phones):
    """Parse the phone number."""
    if not phones:
        return None

    preferred_phone = phones[0]
    for phone in phones:
        if phone.get('type') == 'CELL':
            preferred_phone = phone
            break

    return preferred_phone


def parse_address(addresses):
    """Parse the address."""
    if not addresses:
        return None

    preferred_address = addresses[0]
    for address in addresses:
        if address.get('addressType') == "CURRENT":
            preferred_address = address
            break

    return preferred_address


def extract_salesperson(salespersons):
    """Extract salesperson data."""
    if not salespersons:
        return None

    primary_salesperson = salespersons[0]
    for salesperson in salespersons:
        if salesperson.get("isPrimary") is True:
            primary_salesperson = salesperson
            break

    return {
        "crm_salesperson_id": str(primary_salesperson.get("arcId")),
        "is_primary": True,
        "position_name": primary_salesperson.get("type"),
    }


def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format tekion crm json data to unified format."""
    entries = []
    raw_items = json_data.get('data', [])
    try:
        for item in raw_items:
            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            lead_origin = item.get('source', {}).get('sourceType', '')
            if lead_origin not in ['Internet']:
                logger.info(f"Skipping lead with origin: {lead_origin}")
                continue

            crm_lead_id = item.get('id', '')

            db_lead["crm_lead_id"] = crm_lead_id
            db_lead["lead_ts"] = format_ts(item.get('createdTime', ''))
            db_lead["lead_status"] = item.get('status')
            db_lead["lead_substatus"] = ''
            db_lead["lead_comment"] = item.get('notes', [{}])[0].get('description', '') if item.get('notes') else ''
            db_lead["lead_origin"] = lead_origin.upper()
            db_lead["lead_source"] = item.get('source', {}).get('sourceName', '')
            db_lead["lead_source_detail"] = item.get('source', {}).get('subSource', '')

            vehicles = item.get('vehicles', [])
            trade_ins = item.get('tradeIns', [{}])[0]
            for vehicle in vehicles:
                db_vehicle = {
                    "vin": vehicle.get('vin', ''),
                    "year": int(vehicle.get('year')) if vehicle.get('year') else None,
                    "make": vehicle.get('make', ''),
                    "model": vehicle.get('model', ''),
                    "condition": vehicle.get('stockType', ''),
                    "oem_name": vehicle.get('trimDetails', {}).get('oem', ''),
                    "type": vehicle.get('trimDetails', {}).get('bodyType', ''),
                    "class": vehicle.get('trimDetails', {}).get('bodyClass', ''),
                    "transmission": vehicle.get('trimDetails', {}).get('transmissionControlType', ''),
                    "interior_color": vehicle.get('interiorColor', ''),
                    "exterior_color": vehicle.get('exteriorColor', ''),
                    "trim": vehicle.get('trimDetails', {}).get('trim', ''),
                    "trade_in_vin": trade_ins.get('vehicle', {}).get('vin', ''),
                    "trade_in_year": int(vehicle.get('year')) if vehicle.get('year') else None,
                    "trade_in_make": trade_ins.get('vehicle', {}).get('make', ''),
                    "trade_in_model": trade_ins.get('vehicle', {}).get('model', ''),
                }

                db_vehicle = {key: value for key, value in db_vehicle.items() if value not in [None, ""]}

                db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            consumer = item.get('customers', [{}])[0]
            db_consumer = {
                "first_name": consumer.get('firstName', ''),
                "last_name": consumer.get('lastName', ''),
                "middle_name": consumer.get('middleName', ''),
                "email": parse_email(consumer.get('emails', [])).get("emailId"),
                "phone": parse_phone_number(consumer.get('phones', [])).get("number"),
                "address": parse_address(consumer.get('addresses', [])).get("line1") + " " + parse_address(consumer.get('addresses', [])).get("line2"),
                "city": parse_address(consumer.get('addresses', [])).get("city"),
                "country": parse_address(consumer.get('addresses', [])).get("country"),
                "postal_code": parse_address(consumer.get('addresses', [])).get("zip"),
                "email_optin_flag": parse_email(consumer.get('emails', [])).get("optedForCommunication"),
                "sms_optin_flag": parse_phone_number(consumer.get('phones', [])).get("optedForCommunication"),
            }

            if not db_consumer["email"] and not db_consumer["phone"]:
                logger.warning(f"Email or phone number is required. Skipping lead {crm_lead_id}")
                continue

            db_salesperson = extract_salesperson(item.get('assignees', None))
            db_lead["salespersons"] = [db_salesperson] if db_salesperson else []

            entry = {
                "product_dealer_id": product_dealer_id,
                "lead": db_lead,
                "consumer": db_consumer
            }

            entries.append(entry)
        return entries
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def post_entry(entry: dict, crm_api_key: str, index: int) -> bool:
    """Process a single entry."""
    logger.info(f"[THREAD {index}] Processing entry {entry}")
    try:
        product_dealer_id = entry["product_dealer_id"]
        consumer = entry["consumer"]
        lead = entry["lead"]
        unified_crm_consumer_id = upload_to_db(consumer, f"consumers?dealer_id={product_dealer_id}", crm_api_key, index, "consumer_id")
        lead["consumer_id"] = unified_crm_consumer_id
        unified_crm_lead_id = upload_to_db(lead, "leads", crm_api_key, index, "lead_id")
        logger.info(f"[THREAD {index}] Lead successfully created: {unified_crm_lead_id}")
    except Exception as e:
        if '409' in str(e):
            # Log the 409 error and continue with the next entry
            logger.warning(f"[THREAD {index}] {e}")
        else:
            logger.error(f"[THREAD {index}] Error uploading entry to DB: {e}")
            return False

    return True


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]
        product_dealer_id = key.split('/')[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")

        entries = parse_json_to_entries(product_dealer_id, json_data)
        logger.info(f"Transformed entries: {entries}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        results = []
        # Process each entry in parallel, each entry takes about 8 seconds to process.
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(post_entry,
                                entry, crm_api_key, idx)
                for idx, entry in enumerate(entries)
            ]
            for future in as_completed(futures):
                results.append(future.result())

        for result in results:
            if not result:
                raise Exception("Error detected posting and forwarding an entry")

    except Exception as e:
        logger.error(f"Error transforming tekion crm record - {record}: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw tekion crm data to the unified format."""
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
