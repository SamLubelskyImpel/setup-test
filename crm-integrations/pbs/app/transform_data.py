import boto3
import logging
import requests
from os import environ
from json import loads, dumps
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


class EventListenerError(Exception):
    pass


def get_secret(secret_name, secret_key) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data


def upload_consumer_to_db(consumer: Dict[str, Any], product_dealer_id: str, api_key: str, index: int) -> Any:
    """Upload consumer to the database through CRM API."""
    logger.info(f"Consumer data to send: {consumer}")
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/consumers?dealer_id={product_dealer_id}",
        json=consumer,
        headers={
            "x_api_key": api_key,
            "partner_id": UPLOAD_SECRET_KEY,
        },
    )
    logger.info(
        f"[THREAD {index}] Response from Unified Layer Create Customer {response.status_code} {response.text}",
    )
    response.raise_for_status()
    unified_crm_consumer_id = response.json().get("consumer_id")

    if not unified_crm_consumer_id:
        logger.error(f"Error creating consumer: {consumer}")
        raise Exception(f"Error creating consumer: {consumer}")

    return unified_crm_consumer_id


def upload_lead_to_db(lead: Dict[str, Any], api_key: str, index: int) -> Any:
    """Upload lead to the database through CRM API."""
    logger.info(f"Lead data to send: {lead}")
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/leads",
        json=lead,
        headers={"x_api_key": api_key, "partner_id": UPLOAD_SECRET_KEY},
    )
    logger.info(
        f"[THREAD {index}] Response from Unified Layer Create Lead {response.status_code} {response.text}"
    )
    response.raise_for_status()
    unified_crm_lead_id = response.json().get("lead_id")

    if not unified_crm_lead_id:
        logger.error(f"Error creating lead: {lead}")
        raise Exception(f"Error creating lead: {lead}")

    return unified_crm_lead_id


def format_ts(input_ts: str) -> str:
    """
    Format a timestamp string into a db format.

    Assumes the input timestamp is in the format "November, 17 2023 18:57:17".
    """
    dt = datetime.strptime(input_ts, "%Y-%m-%dT%H:%M:%S.%fZ")

    # Format the datetime object to ISO 8601 UTC format
    output_format = "%Y-%m-%dT%H:%M:%SZ"
    return dt.strftime(output_format)


def extract_contact_information(item_name: str, item: Any, db_entity: Any) -> None:
    """Extract contact information from the pbs json data."""

    if item_name == 'consumer':
        # db_entity[f'crm_{item_name}_id'] = item.get('userID')
        db_entity["first_name"] = item.get('ContactFirstName')
        db_entity["middle_name"] = item.get('ContactMiddleName', '')
        db_entity["last_name"] = item.get('ContactLastName')
        # emails = item.get('contactInformation', {}).get('emails', [])
        db_entity["email"] = item.get('ContactEmailAddress', '')
        # phone_numbers = item.get('contactInformation', {}).get('phoneNumbers', [])
        db_entity["phone"] = item.get("ContactCellPhone", '')
        
        # addresses = item.get('contactInformation', {}).get('addresses', [])
        # address = addresses[0] if addresses else None

        db_entity["address"] = item.get('ContactAddress')
        db_entity["city"] = item.get('ContactCity')
        db_entity["postal_code"] = item.get('ContactZipCode')

        # communication_preferences = item.get('ContactPreferredContactMethods', [])
        communication_preferences = item.get('ContactCommunicationPreferences', {})

        #TODO: Determine if there are positive values besides Implied Consent
        if communication_preferences:
            db_entity["email_optin_flag"] = True if communication_preferences.get('Email') == "ImpliedConsent" else False
            db_entity["sms_optin_flag"] = True if communication_preferences.get('TextMessage') == "ImpliedConsent" else False

    if item_name == 'salesperson':
        db_entity["crm_salesperson_id"] = item.get('EmployeeRef')
        db_entity["first_name"] = item.get('Name')
        db_entity["position_name"] = item.get('Role')

    # Remove None values from db_entity without reassignment
    keys_to_remove = [key for key, value in db_entity.items() if value is None]
    for key in keys_to_remove:
        del db_entity[key]

#TODO: Clarify field transformations
def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format pbs json data to unified format."""
    entries = []
    try:
        for item in json_data:
            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            #TODO: Clarify lead origins
            # lead_origin = item.get('source', {}).get('source', '')
            # if lead_origin not in ['Internet', 'Third Party']:
            #     logger.info(f"Skipping lead with origin: {lead_origin}")
            #     continue

            crm_lead_id = item.get('DealId', '')
            db_lead["crm_lead_id"] = crm_lead_id
            db_lead["lead_ts"] = format_ts(item.get('DealCreationDate')[:25]+'Z')

            db_lead["lead_status"] = item.get('DealStatus')
            db_lead["lead_substatus"] = item.get('SystemStatus')
            db_lead["lead_comment"] = item.get('ContactNotes')
            # db_lead["lead_origin"] = lead_origin.upper()

            # provider_name = (
            #     item.get('costItem', {}).get('provider', {}).get('provider') or
            #     item.get('manufacturer', {}).get('name')
            # )

            # db_lead["lead_source"] = provider_name if provider_name else None

            # vehicles = item.get('Vehicles', [])
            # for vehicle in vehicles:
            is_new = False if item.get("VehicleStatus")=="Used" else True
            db_vehicle = {
                "crm_vehicle_id": item.get('carID'),
                "vin": item.get('VehicleVIN'),
                "manufactured_year": int(item.get('VehicleYear')) if item.get('VehicleYear') else None,
                "make": item.get('VehicleMake'),
                "model": item.get('VehicleModel'),
                "condition": 'New' if is_new is True else ('Used' if is_new is False else None)
            }
            db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}

            db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            # consumer = item.get('customer', None)
            extract_contact_information('consumer', item, db_consumer)

            if not db_consumer["email"] and not db_consumer["phone"]:
                logger.warning(f"Email or phone number is required. Skipping lead {crm_lead_id}")
                continue

            salespersons = item.get('DealUserRoles', [])
            salesperson = {}
            if len(salespersons) == 1:
                salesperson = salespersons[0]
            else:
                for person in salespersons:
                    if person.get('Primary') == True:
                        salesperson = person
                        break

            extract_contact_information('salesperson', salesperson, db_salesperson)
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
        unified_crm_consumer_id = upload_consumer_to_db(consumer, product_dealer_id, crm_api_key, index)
        lead["consumer_id"] = unified_crm_consumer_id
        unified_crm_lead_id = upload_lead_to_db(lead, crm_api_key, index)
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
        logger.error(f"Error transforming pbs record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
            product_dealer_id, product_dealer_id, entries[0]['lead']['crm_lead_id'], e)
            )
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
