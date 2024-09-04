import boto3
import logging
import requests
from os import environ
from json import loads
from typing import Any, Dict
import pandas as pd
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

def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format pbs json data to unified format."""
    entries = []
    try:
        for deal in json_data:

            contact = deal.get("Contact_Info", {})
            vehicle = deal.get("Vehicle_Info", {})

            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            # Skip leads missing critical data

            crm_lead_id = deal.get('DealId', '')
            if not crm_lead_id:
                logger.warning(f"DealId is required. Skipping lead.")
                continue
            
            if not deal.get('CreationDate', ''):
                logger.warning(f"Deal Creation Date is required. Skipping lead {crm_lead_id}")
                continue

            if not deal.get('LeadType', ''):
                logger.warning(f"Lead Type is required. Skipping lead {crm_lead_id}")
                continue

            # Parse Lead Data
            
            # Format timestamp to database accepted format
            output_format = "%Y-%m-%dT%H:%M:%SZ"
            db_lead["lead_ts"] = pd.to_datetime(deal.get('CreationDate'), format="%Y-%m-%dT%H:%M:%S.%fZ").strftime(output_format)
            
            db_lead["crm_lead_id"] = crm_lead_id[:5000]
            db_lead["lead_status"] = deal.get('Status', '')
            db_lead["lead_substatus"] = deal.get('SystemStatus', '')
            db_lead["lead_comment"] = deal.get('Notes', '')[:5000]
            db_lead["lead_origin"] = deal.get('LeadType')
            db_lead["lead_source"] = deal.get('LeadSource', '')


            # Parse Vehicle Data 

            db_vehicle = {
                "crm_vehicle_id": vehicle.get('VehicleId', None),
                "vin": vehicle.get('VIN', None),
                "year": int(vehicle.get('Year', '')) if vehicle.get('Year', '') else None,
                "make": vehicle.get('Make', None),
                "model": vehicle.get('Model', None),
                "status": vehicle.get("Status", None),
                "stock_num": vehicle.get('StockNumber', None),
                "type": vehicle.get('Type', None),
                "mileage": vehicle.get('Odometer', None),
                "transmission": vehicle.get('Transmission', None),
                "interior_color": vehicle.get('InteriorColor', {}).get('Description', None),
                "exterior_color": vehicle.get('ExteriorColor', {}).get('Description', None),
                "trim": vehicle.get('Trim', None),
                "vehicle_comments": vehicle.get('Notes', None)
            }

            vehicles = deal.get('Vehicles', [])
            trades = deal.get('Trades', [])

            if len(vehicles) > 0:
                vehicle = vehicles[0]
                db_vehicle["price"] = vehicle.get('Cost', None)
                db_vehicle["condition"] = "New" if vehicle.get('IsNewVehicle', False) else "Used"
                
            if len(trades) > 0:
                trade = trades[0]
                db_vehicle["trade_in_vin"] = trade.get('VIN', None)

            logger.info(f"Vehicle Data: {db_vehicle}")
            
            db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}

            db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            # Parse Consumer Data

            db_consumer = {
                "crm_consumer_id": contact.get('ContactId'),
                "first_name": contact.get('FirstName'),
                "middle_name": contact.get('MiddleName', ''),
                "last_name":contact.get('LastName'),
                "email": contact.get('EmailAddress', ''),
                "phone": contact.get("CellPhone", ''),
                "address": contact.get('Address'),
                "city": contact.get('City'),
                "postal_code": contact.get('ContactZipCode')
            }

            communication_preferences = contact.get('ContactCommunicationPreferences', {})

            if communication_preferences:
                db_consumer["email_optin_flag"] = True if communication_preferences.get('Email') == "ImpliedConsent" or communication_preferences.get('Email') == "ExpressedConsent" else False
                db_consumer["sms_optin_flag"] = True if communication_preferences.get('TextMessage') == "ExpressedConsent" else False

            if not db_consumer["email"] and not db_consumer["phone"]:
                logger.warning(f"Email or phone number is required. Skipping lead {crm_lead_id}")
                continue

            db_consumer = {key: value for key, value in db_consumer.items() if value is not None}

            # Parse Salesperson Data

            salespersons = deal.get('DealUserRoles', [])
            salesperson = {}
            if len(salespersons) == 1:
                salesperson = salespersons[0]
            else:
                for person in salespersons:
                    if person.get('Primary', '') == True:
                        salesperson = person
                        break

            db_salesperson = {
                    "crm_salesperson_id": salesperson.get('EmployeeRef', None),
                    "first_name": salesperson.get('Name', None),
                    "position_name": salesperson.get('Role', None)
            }

            db_salesperson = {key: value for key, value in db_salesperson.items() if value is not None}

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
