import boto3
import requests
from json import loads
from os import environ
import logging
from typing import Any
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

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


def get_existing_lead(crm_lead_id, crm_dealer_id, crm_api_key):
    """Get existing lead from CRM API."""
    try:
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            params={"crm_dealer_id": crm_dealer_id, "integration_partner_name": "MOMENTUM"},
        )
        status_code = response.status_code
        if status_code == 200:
            return response.json()["lead_id"]
        elif status_code == 404:
            return None
        else:
            raise Exception(f"Error getting existing lead from CRM API: {response.text}")

    except Exception as e:
        logger.error(f"Error getting existing lead from CRM API: {e}")
        raise


def create_consumer(parsed_lead, crm_api_key) -> dict:
    """Create consumer in db."""
    consumer_obj = parsed_lead["consumer"]

    try:
        response = requests.post(
            url=f"https://{CRM_API_DOMAIN}/consumers",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            params={"dealer_id": parsed_lead["product_dealer_id"]},
            json=consumer_obj,
        )
        response.raise_for_status()
        logger.info(f"CRM API /consumers responded with: {response.status_code}")
        return response.json()
    except Exception as e:
        logger.error(f"Error creating consumer from CRM API: {e}")
        raise


def create_lead(parsed_lead, consumer_id, crm_api_key) -> dict:
    """Create lead in db."""
    lead_obj = parsed_lead["lead"]

    lead_obj.update({
        "consumer_id": consumer_id,
        "vehicles_of_interest": [parsed_lead["vehicle"]] if parsed_lead["vehicle"] else [],
        "salespersons": [parsed_lead["salesperson"]] if parsed_lead["salesperson"] else [],
    })

    try:
        response = requests.post(
            url=f"https://{CRM_API_DOMAIN}/leads",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            json=lead_obj,
        )
        response.raise_for_status()
        logger.info(f"CRM API /leads responded with: {response.status_code}")
        return response.json()
    except Exception as e:
        logger.error(f"Error creating lead from CRM API: {e}")
        raise


def parse_phone_number(data):
    """Parse the phone number."""
    phone_numbers = [data.get("cellPhone", ""), data.get("homePhone", ""), data.get("workPhone", "")]
    phone_number = next((number for number in phone_numbers if number), "")

    return ''.join(char for char in phone_number if char.isdigit())


def parse_address(data):
    """Parse the address."""
    add1 = data.get("address1")
    add2 = data.get("address2")
    if add1 and add2:
        return f"{add1} {add2}"
    else:
        return add1 or add2 or None


def extract_salesperson(salesperson_id: str, salesperson_name=None, position_name="Primary Salesperson"):
    """Extract salesperson data."""
    db_salesperson = {
        "crm_salesperson_id": salesperson_id,
        "first_name": "",
        "last_name": "",
        "is_primary": True,
        "position_name": position_name,
    }
    if salesperson_name:
        try:
            sp_first, sp_last = salesperson_name.split()
            db_salesperson["first_name"] = sp_first
            db_salesperson["last_name"] = sp_last
        except ValueError:
            logger.warning(f"Unexpected salesperson name: {salesperson_name}")
            db_salesperson["first_name"] = salesperson_name.strip().replace(" ", "")
            db_salesperson["last_name"] = ""

    return db_salesperson


def parse_lead(product_dealer_id, data):
    """Parse the JSON lead data."""
    parsed_data = {}
    try:
        crm_lead_id = data["id"]

        db_consumer = {
            "first_name": data.get("firstName"),
            "last_name": data.get("lastName"),
            "phone": parse_phone_number(data),
            "email": data.get("email"),
            "address": parse_address(data),
            "city": data.get("city"),
            "country": data.get("country"),
            "postal_code": str(data.get("zip", "")) if data.get("zip") else None
        }

        if not db_consumer["email"] and not db_consumer["phone"]:
            raise Exception("Email or phone number is required")

        db_consumer = {key: value for key, value in db_consumer.items() if value}

        db_lead = {
            "lead_ts": data.get("date", datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')),  # 2024-01-26T17:16:19-0800
            "crm_lead_id": crm_lead_id,
            "lead_status": data["leadStatus"],
            "lead_substatus": "",
            "lead_comment": data.get("comments", ""),
            "lead_origin": data["leadType"],
            "lead_source": data.get("providerName", ""),
        }

        vehicleType = data.get("vehicleType")
        if vehicleType:
            vehicleType = vehicleType.lower().capitalize()
            if vehicleType not in ("New", "Used"):
                logger.warning(f"Unexpected vehicle type: {vehicleType}")

        db_vehicle = {
            "vin": data.get("vin"),
            "stock_num": data.get("stock"),
            "make": data.get("make"),
            "model": data.get("model"),
            "year": data.get("year"),
            "exterior_color": data.get("color"),
            "trim": data.get("trim"),
            "condition": vehicleType
        }
        db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}

        if data.get("bdcID"):
            db_salesperson = extract_salesperson(data["bdcID"], data.get("bdcName"), "BDC Rep")

        elif data.get("contactID"):
            logger.info("No BDC rep found. Using sales rep as salesperson.")
            db_salesperson = extract_salesperson(data["contactID"], data.get("contactName"))

        else:
            db_salesperson = {}
            logger.info("No salesperson found in the lead data")

        parsed_data = {
            "product_dealer_id": product_dealer_id,
            "lead": db_lead,
            "consumer": db_consumer,
            "vehicle": db_vehicle,
            "salesperson": db_salesperson
        }

        return parsed_data

    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
        raise


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

        crm_lead_id = json_data["id"]
        crm_dealer_id = json_data["dealerID"]

        parsed_lead = parse_lead(product_dealer_id, json_data)
        logger.info(f"Transformed record body: {parsed_lead}")
        if parsed_lead["lead"]["lead_origin"] != "Internet":
            logger.warning(f"Lead type is not Internet: {parsed_lead['lead']['lead_origin']}. Ignoring lead.")
            return

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]
        existing_lead = get_existing_lead(crm_lead_id, crm_dealer_id, crm_api_key)
        if existing_lead:
            logger.warning(f"Existing lead detected: DB Lead ID {existing_lead}. Ignoring duplicate lead.")
            return

        consumer_id = create_consumer(parsed_lead, crm_api_key)["consumer_id"]
        lead_id = create_lead(parsed_lead, consumer_id, crm_api_key)["lead_id"]

        logger.info(f"New lead created: {lead_id}")

    except Exception as e:
        logger.error(f"Error transforming momentum record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, crm_lead_id, e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw momentum data to the unified format."""
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
