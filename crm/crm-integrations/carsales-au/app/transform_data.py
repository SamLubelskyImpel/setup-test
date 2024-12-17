"""Transform raw carsales data to the unified format."""

import boto3
import logging
import requests
from os import environ
from json import loads, dumps
from typing import Any, Dict
from datetime import datetime
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

def extract_value(value_type, items, key):
    keyname, valuename = '',''
    if value_type == "Attributes":
        keyname, valuename = 'Name', 'Value'
    elif value_type == "Colours":
        keyname, valuename = 'Location', 'Name'
    elif value_type == "Identification":
        keyname, valuename = 'Type', 'Value'
    elif value_type == "PhoneNumbers":
        keyname, valuename = "Type", 'Number'
    for item in items:
        if item.get(keyname) == key:
            return item.get(valuename)
    return None


def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format carsales json data to unified format."""
    try:
        db_lead = {}
        db_vehicles = []
        db_consumer = {}
        db_salesperson = {}

        crm_lead_id = json_data["Identifier"]
        db_lead["crm_lead_id"] = crm_lead_id
        db_lead["lead_ts"] = json_data.get('CreatedUtc')
        db_lead["lead_status"] = json_data.get('Status', '')
        db_lead["lead_substatus"] = ''
        db_lead["lead_comment"] = json_data.get('Comments', '')
        db_lead["lead_origin"] = 'Internet'
        db_lead["lead_source"] = 'CarSales'


        vehicle = json_data.get('Item', {})
        identification = vehicle.get('Identification', [])
        colors = vehicle.get('Colours', [])
        specification = vehicle.get('Specification', {})
        attributes = specification.get('Attributes', [])


        db_vehicle = {
            "stock_num": extract_value("Identification", identification, "StockNumber"),
            "year": int(specification.get('ReleaseDate').get("Year")) if specification.get('ReleaseDate', {}).get("Year", None) else None,
            "make": specification.get('Make', None),
            "model": specification.get('Model', None),
            "body_style": extract_value("Attributes", attributes, "BodyStyle"),
            "exterior_color": extract_value("Colours", colors, "Exterior"),
            "interior_color": extract_value("Colours", colors, "Interior"),
            "condition": vehicle.get('ListingType', None),
            "transmission": extract_value("Attributes", attributes, "Transmission"),
            "status": vehicle.get('SaleStatus', None)
        }
        price_list = vehicle.get("PriceList", [])
        if price_list:
            db_vehicle["price"] = float(price_list[-1].get("Amount")) if price_list[-1].get("Amount", None) else None
        
        odometer = vehicle.get("OdometerReadings", [])
        if odometer:
            db_vehicle["odometer_units"] = odometer[0].get("UnitsOfMeasure", None)
            db_vehicle["mileage"] = int(odometer[-1].get("Value")) if odometer[-1].get("Value") else 0

        db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}

        db_vehicles.append(db_vehicle)

        db_lead["vehicles_of_interest"] = db_vehicles

        consumer = json_data.get('Prospect', None)

        if not consumer:
            logger.warning(f"No Consumer provided for lead {crm_lead_id}")
            raise

        phone_number = extract_value("PhoneNumbers", consumer.get("PhoneNumbers", []), "Home")
        if not phone_number:
            phone_number = extract_value("PhoneNumbers", consumer.get("PhoneNumbers", []), "Mobile")

        db_consumer = {
            "first_name": consumer.get('FirstName', None),
            "last_name": consumer.get('LastName', None),
            "email": consumer.get('Email', None),
            "phone": phone_number
        }

        if not db_consumer["email"] and not db_consumer["phone"]:
            logger.warning(f"Email or phone number is required. Cannot save lead {crm_lead_id}")
            raise

        db_consumer = {key: value for key, value in db_consumer.items() if value is not None}

        salesperson = json_data.get('Assignment', None)
        if salesperson: 
            salesperson_name = salesperson.get('Name').split() if salesperson.get('Name', None) else None
            db_salesperson = {
                "first_name": salesperson_name[0] if salesperson_name else None,
                "last_name": salesperson_name[1] if salesperson_name and len(salesperson_name) > 1 else None,
                "email": salesperson.get('Email', None)
            }

        db_salesperson = {key: value for key, value in db_salesperson.items() if value is not None}

        db_lead["salespersons"] = [db_salesperson]

        entry = {
            "product_dealer_id": product_dealer_id,
            "lead": db_lead,
            "consumer": db_consumer
        }

        return entry
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def post_entry(entry: dict, crm_api_key: str) -> bool:
    """Process a single entry."""
    logger.info(f"Processing entry {entry}")
    try:
        product_dealer_id = entry["product_dealer_id"]
        consumer = entry["consumer"]
        lead = entry["lead"]
        unified_crm_consumer_id = upload_consumer_to_db(consumer, product_dealer_id, crm_api_key)
        lead["consumer_id"] = unified_crm_consumer_id
        unified_crm_lead_id = upload_lead_to_db(lead, crm_api_key)
        logger.info(f"Lead successfully created: {unified_crm_lead_id}")
    except Exception as e:
        if '409' in str(e):
            # Log the 409 error and continue with the next entry
            logger.warning(f"{e}")
        else:
            logger.error(f"Error uploading entry to DB: {e}")
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

        dealer_id = json_data["Seller"]["Identifier"]

        entry = parse_json_to_entries(product_dealer_id, json_data)
        logger.info(f"Transformed entry: {entry}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        result = post_entry(entry, crm_api_key)
        if not result:
            raise Exception ("Error occurred uploading lead to DB")

    except Exception as e:
        logger.error(f"Error transforming carsales au record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nTraceback: {}".format(
            product_dealer_id, dealer_id, e)
            )
        raise


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