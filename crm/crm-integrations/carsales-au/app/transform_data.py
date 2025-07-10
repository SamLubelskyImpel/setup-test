"""Transform raw carsales data to the unified format."""

import boto3
import logging
import requests
from os import environ
from json import loads
from typing import Any, Dict
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





class EventListenerError(Exception):
    pass


def get_secret(secret_name, secret_key) -> Any:
    """Get secret from Secrets Manager."""
    sm_client = boto3.client('secretsmanager')
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    
    return secret_data


def upload_consumer_to_db(consumer: Dict[str, Any], product_dealer_id: str, api_key: str) -> Any:
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
        f"Response from Unified Layer Create Customer {response.status_code} {response.text}",
    )
    response.raise_for_status()
    unified_crm_consumer_id = response.json().get("consumer_id")

    if not unified_crm_consumer_id:
        logger.error(f"Error creating consumer: {consumer}")
        raise Exception(f"Error creating consumer: {consumer}")

    return unified_crm_consumer_id


def upload_lead_to_db(lead: Dict[str, Any], api_key: str) -> Any:
    """Upload lead to the database through CRM API."""
    logger.info(f"Lead data to send: {lead}")
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/leads",
        json=lead,
        headers={"x_api_key": api_key, "partner_id": UPLOAD_SECRET_KEY},
    )
    logger.info(
        f"Response from Unified Layer Create Lead {response.status_code} {response.text}"
    )
    response.raise_for_status()
    unified_crm_lead_id = response.json().get("lead_id")

    if not unified_crm_lead_id:
        logger.error(f"Error creating lead: {lead}")
        raise Exception(f"Error creating lead: {lead}")

    return unified_crm_lead_id


def create_lead(lead_body: dict, crm_api_key: str):
    """Create lead in the CRM API."""
    logger.info(f"Processing lead: {lead_body}")
    try:
        product_dealer_id = lead_body["product_dealer_id"]
        consumer = lead_body["consumer"]
        lead = lead_body["lead"]
        unified_crm_consumer_id = upload_consumer_to_db(consumer, product_dealer_id, crm_api_key)
        lead["consumer_id"] = unified_crm_consumer_id
        unified_crm_lead_id = upload_lead_to_db(lead, crm_api_key)
        logger.info(f"Lead successfully created: {unified_crm_lead_id}")

    except Exception as e:
        logger.error(f"Error uploading lead to DB: {e}")
        raise e


def extract_value(value_type, items, key):
    keyname, valuename = '', ''
    if value_type == "Identification":
        keyname, valuename = 'Type', 'Value'
    elif value_type == "PhoneNumbers":
        keyname, valuename = "Type", 'Number'
    for item in items:
        if item.get(keyname) == key:
            return item.get(valuename)
    return None


def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format carsales json data to unified format."""
    def name_split(name):
        try:
            first_name, last_name = name.split()
        except ValueError:
            logger.warning(f"Name is not in the expected format: {name}")
            first_name = name
            last_name = ""
        return first_name, last_name

    try:
        db_lead = {}
        db_vehicles = []
        db_consumer = {}
        db_salesperson = {}

        # Parse Lead
        crm_lead_id = json_data["Identifier"]
        db_lead["crm_lead_id"] = crm_lead_id
        db_lead["lead_ts"] = json_data.get('CreatedUtc')
        db_lead["lead_status"] = json_data.get('Status', '')
        db_lead["lead_substatus"] = ''
        db_lead["lead_comment"] = json_data.get('Comments', '')
        db_lead["lead_origin"] = 'Internet'
        db_lead["lead_source"] = 'CarSales'

        # Parse Vehicle
        vehicle = json_data.get('Item', {})
        identification = vehicle.get('Identification', [])
        specification = vehicle.get('Specification', {})
        listing_type = vehicle.get('ListingType', '')
        if listing_type in ('New', 'Showroom'):
            condition = 'New'
        elif listing_type == 'Used':
            condition = 'Used'
        else:
            condition = None

        db_vehicle = {
            "stock_num": extract_value("Identification", identification, "StockNumber"),
            "year": int(specification.get('ReleaseDate').get("Year")) if specification.get('ReleaseDate', {}).get("Year", None) else None,
            "make": specification.get('Make', None),
            "model": specification.get('Model', None),
            "condition": condition
        }
        price_list = vehicle.get("PriceList", [])
        if price_list:
            db_vehicle["price"] = float(price_list[-1].get("Amount")) if price_list[-1].get("Amount", None) else None

        db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}
        db_vehicles.append(db_vehicle)

        db_lead["vehicles_of_interest"] = db_vehicles

        # Parse Consumer
        consumer = json_data.get('Prospect', None)
        if not consumer:
            logger.warning(f"No Consumer provided for lead {crm_lead_id}")
            raise Exception(f"No Consumer provided for lead {crm_lead_id}")

        phone_number = extract_value("PhoneNumbers", consumer.get("PhoneNumbers", []), "Home")
        if not phone_number:
            phone_number = extract_value("PhoneNumbers", consumer.get("PhoneNumbers", []), "Mobile")

        customer_name = consumer.get('Name', '')
        first_name, last_name = name_split(customer_name)

        db_consumer = {
            "first_name": first_name,
            "last_name": last_name,
            "email": consumer.get('Email', None),
            "phone": phone_number
        }

        if not db_consumer["email"] and not db_consumer["phone"]:
            logger.warning(f"Email or phone number is required. Cannot save lead {crm_lead_id}")
            raise Exception(f"Email or phone number is required. Cannot save lead {crm_lead_id}")

        db_consumer = {key: value for key, value in db_consumer.items() if value is not None}

        # Parse Salesperson
        salesperson = json_data.get('Assignment', None)
        if salesperson:
            salesperson_name = salesperson.get('Name', '')
            sales_first_name, sales_last_name = name_split(salesperson_name)
            db_salesperson = {
                "first_name": sales_first_name,
                "last_name": sales_last_name,
                "email": salesperson.get('Email', None)
            }
            db_salesperson = {key: value for key, value in db_salesperson.items() if value is not None}
            db_lead["salespersons"] = [db_salesperson]
        else:
            db_lead["salespersons"] = []

        lead_body = {
            "product_dealer_id": product_dealer_id,
            "lead": db_lead,
            "consumer": db_consumer
        }

        return lead_body
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    s3_client = boto3.client("s3")
    
    product_dealer_id = None
    crm_dealer_id = None
    try:
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]
        product_dealer_id = key.split('/')[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")

        crm_dealer_id = json_data["Seller"]["Identifier"]

        lead_body = parse_json_to_entries(product_dealer_id, json_data)
        logger.info(f"Transformed lead: {lead_body}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        create_lead(lead_body, crm_api_key)

    except Exception as e:
        logger.error(f"Error transforming carsales au record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, e)
            )
        raise e


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw carsales data to the unified format."""
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
