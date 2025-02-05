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
PARTNER_KEY = environ.get("PARTNER_KEY")

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


def parse_email(email_communications):
    """Extract the preferred email or return a default empty email."""
    default_email = {
        "email": "",
        "optedForCommunication": False
    }

    if not email_communications:
        return default_email

    preferred_email = email_communications[0]

    for email in email_communications:
        if email.get("usagePreference", {}).get("preferred", False):
            preferred_email = email
            break

    return {
        "email": preferred_email.get("email", ""),
        "optedForCommunication": preferred_email.get("usagePreference", {}).get("preferred", False)
    }


def parse_phone_number(phone_communications):
    """Extract the preferred phone number or return a default empty number."""
    default_phone = {
        "number": "",
        "optedForCommunication": False
    }

    if not phone_communications:
        return default_phone

    preferred_phone = phone_communications[0]

    for phone in phone_communications:
        if phone.get("usagePreference", {}).get("preferred", False):
            preferred_phone = phone
            break

    return {
        "number": preferred_phone.get("phone", {}).get("completeNumber", ""),
        "optedForCommunication": preferred_phone.get("usagePreference", {}).get("preferred", False)
    }


def parse_address(residence):
    """Extracts address details from the residence field."""
    default_address = {
        "line1": "",
        "line2": "",
        "city": "",
        "country": "",
        "zip": ""
    }

    if not residence:
        return default_address

    address = residence.get("address", {})
    return {
        "line1": address.get("addressLine1", "") or default_address["line1"],
        "line2": address.get("addressLine2", "") or default_address["line2"],
        "city": next(
            (unit["value"] for unit in address.get("locationUnits", []) if unit["unit"] == "City"),
            default_address["city"]
        ),
        "country": address.get("addressCountry", "") or default_address["country"],
        "zip": address.get("postalCode", "") or default_address["zip"]
    }


def extract_salesperson(salespersons):
    """Extract salesperson data."""
    if not salespersons:
        return None

    primary_salesperson = salespersons[0]
    for salesperson in salespersons:
        if salesperson.get("isPrimary", False):
            primary_salesperson = salesperson
            break

    return {
        "crm_salesperson_id": str(primary_salesperson.get("id", "")),
        "is_primary": True,
        "position_name": primary_salesperson.get("role", "")
    }

def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format tekion crm json data to unified format."""
    entries = []
    logger.info(f"Total leads received: {len(json_data)}")  # Log incoming data count
    try:
        for item in json_data:
            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            lead_origin = item.get('source', {}).get('sourceType', '').upper()
            if lead_origin not in ['INTERNET', 'OEM']:
                logger.warning(f"Skipping lead {item.get('id', 'UNKNOWN_ID')} due to invalid origin: {lead_origin}")
                continue

            # Extract crm_dealer_id, placed in lead data by invoke.
            crm_dealer_id = item.get('impel_crm_dealer_id')
            crm_lead_id = item.get('id', '')

            db_lead["crm_lead_id"] = crm_lead_id
            db_lead["lead_ts"] = format_ts(item.get('createdTime', ''))
            db_lead["lead_status"] = item.get('status')
            db_lead["lead_substatus"] = ''
            db_lead["lead_comment"] = (item.get('notes', [{}])[0].get('description', '') if item.get('notes') else '')[:5000]
            db_lead["lead_origin"] = lead_origin.upper()
            db_lead["lead_source"] = item.get('source', {}).get('sourceName', '')
            db_lead["lead_source_detail"] = item.get('source', {}).get('subSource', '')

            vehicles = item.get('vehicles', [])
            trade_ins = item.get('tradeIns', [{}])[0] if item.get('tradeIns') else {}
            if not vehicles: 
                logger.warning(f"Lead {crm_lead_id}: No vehicles found, setting vehicles_of_interest to an empty list.")
            else:
                for vehicle in vehicles:
                    vehicle_spec = vehicle.get('vehicleSpecification', {})
                    trim_details = vehicle_spec.get('trimDetails', {})
                    trade_in_vehicle = trade_ins.get('vehicle', {})
                    trade_in_spec = trade_in_vehicle.get('vehicleSpecification', {})

                    interior_color = ""
                    exterior_color = ""
                    for color in vehicle_spec.get("vehicleColors", []):
                        if color.get("type") == "INTERIOR":
                            interior_color = color.get("colour", "")
                        elif color.get("type") == "EXTERIOR":
                            exterior_color = color.get("colour", "")

                    db_vehicle = {
                        "vin": vehicle.get('vin', ''),
                        "year": int(vehicle_spec.get('year')) if vehicle_spec.get('year') else None,
                        "make": vehicle_spec.get('make', ''),
                        "model": vehicle_spec.get('model', ''),
                        "condition": vehicle.get('stockType', ''),
                        "oem_name": item.get('oemName', ''),
                        "type": trim_details.get('bodyType', ''),
                        "class": vehicle_spec.get('bodyClass', ''),
                        "transmission": trim_details.get('transmissionControlType', ''),
                        "interior_color": interior_color,
                        "exterior_color": exterior_color,
                        "trim": trim_details.get('trimCode', ''),
                        "trade_in_vin": trade_in_vehicle.get('vin', ''),
                        "trade_in_year": int(trade_in_spec.get('year')) if trade_in_spec.get('year') else None,
                        "trade_in_make": trade_in_spec.get('make', ''),
                        "trade_in_model": trade_in_spec.get('model', ''),
                    }

                    db_vehicle = {key: value for key, value in db_vehicle.items() if value not in [None, ""]}
                    db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            consumer = item.get('contacts', [{}])[0]
            address = parse_address(consumer.get('customerDetails', {}).get('residence', {}))
            email = parse_email(consumer.get('customerDetails', {}).get('emailCommunications', []))
            phone = parse_phone_number(consumer.get('customerDetails', {}).get('phoneCommunications', []))

            db_consumer = {
                "first_name": consumer.get('customerDetails', {}).get('name', {}).get('firstName', ''),
                "last_name": consumer.get('customerDetails', {}).get('name', {}).get('lastName', ''),
                "middle_name": consumer.get('customerDetails', {}).get('name', {}).get('middleName', ''),
                "email": email["email"],
                "phone": phone["number"],
                "address": f"{address['line1']} {address['line2']}".strip(),
                "city": address["city"],
                "country": address["country"],
                "postal_code": address["zip"],
                "email_optin_flag": email["optedForCommunication"],
                "sms_optin_flag": phone["optedForCommunication"]
            }

            if not db_consumer["email"] and not db_consumer["phone"]:
                logger.warning(f"Email or phone number is required. Skipping lead {crm_lead_id}")
                continue

            db_salesperson = extract_salesperson(item.get('assignees', None))
            db_lead["salespersons"] = [db_salesperson] if db_salesperson else []

            entry = {
                "product_dealer_id": product_dealer_id,
                "lead": db_lead,
                "consumer": db_consumer,
                "crm_dealer_id": crm_dealer_id
            }
            entries.append(entry)
            logger.info(f"Successfully added lead {crm_lead_id} to entries.")
        logger.info(f"Total transformed entries: {len(entries)}")
        return entries
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def get_lead(crm_lead_id: str, crm_dealer_id: str, crm_api_key: str) -> Any:
    """Check if lead exists through CRM API."""
    queryStringParameters = f"crm_dealer_id={crm_dealer_id}&integration_partner_name={PARTNER_KEY}"
    url = f'https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}?{queryStringParameters}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    response = requests.get(url, headers=headers)
    logger.info(f"CRM API Get Lead responded with: {response.status_code}")

    if response.status_code == 200:
        response_data = response.json()
        lead_id = response_data.get('lead_id')
        return lead_id
    elif response.status_code == 404:
        logger.info(f"Lead with crm_lead_id {crm_lead_id} not found.")
        return None
    elif response.status_code == 400:
        logger.info(f"Multiple leads found with crm_lead_id {crm_lead_id}.")
        return crm_lead_id
    else:
        logger.error(f"Error getting lead with crm_lead_id {crm_lead_id}: {response.text}")
        raise


def post_entry(entry: dict, crm_api_key: str, index: int) -> bool:
    """Process a single entry."""
    logger.info(f"[THREAD {index}] Processing entry {entry}")
    try:
        product_dealer_id = entry["product_dealer_id"]
        consumer = entry["consumer"]
        lead = entry["lead"]
        crm_dealer_id = entry["crm_dealer_id"]
        crm_lead_id = lead.get("crm_lead_id", "")

        # Check for existing lead
        if crm_lead_id:
            existing_lead = get_lead(lead["crm_lead_id"], crm_dealer_id, crm_api_key)
            if existing_lead:
                logger.warning(f"[THREAD {index}] Lead already exists: {crm_lead_id} for dealer {product_dealer_id}. Skipping entry.")
                return True

        unified_crm_consumer_id = upload_to_db(consumer, f"consumers?dealer_id={product_dealer_id}", crm_api_key, index, "consumer_id")
        lead["consumer_id"] = unified_crm_consumer_id
        unified_crm_lead_id = upload_to_db(lead, "leads", crm_api_key, index, "lead_id")
        logger.info(f"[THREAD {index}] Lead successfully created: {unified_crm_lead_id}")

    except Exception as e:
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, crm_lead_id, e)
        )
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
