"""Create new activix lead in the Impel CRM persistence layer."""
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
            params={"crm_dealer_id": crm_dealer_id, "integration_partner_name": "ACTIVIX"},
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
        "vehicles_of_interest": parsed_lead["vehicles"],
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


def parse_phone_number(phones):
    """Parse the phone number."""
    if not phones:
        return None

    preferred_phone = phones[0]
    for phone in phones:
        if phone.get('type') == 'mobile' and phone.get('valid') is True:
            preferred_phone = phone
            break 

    return preferred_phone.get('number')


def parse_email(emails):
    """Parse the emails."""
    if not emails:
        return None

    preferred_email = emails[0]

    for email in emails:
        if email.get('valid') is True:
            preferred_email = email
            break

    return preferred_email.get('address')


def parse_address(data):
    """Parse the address."""
    add1 = data.get("address_line1")
    add2 = data.get("address_line2")
    if add1 and add2:
        return f"{add1} {add2}"
    else:
        return add1 or add2 or None


def extract_salesperson(position_name, data):
    """Extract salesperson data."""
    return {
        "crm_salesperson_id": str(data.get("id")),
        "first_name": data.get("first_name"),
        "last_name": data.get("last_name"),
        "email": data.get("email"),
        "is_primary": True,
        "position_name": position_name
    }


def parse_lead(product_dealer_id, data):
    """Parse the JSON lead data."""
    parsed_data = {}
    try:
        crm_lead_id = str(data["id"])

        db_consumer = {
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "crm_consumer_id": str(data.get("customer_id")),
            "phone": parse_phone_number(data.get("phones", [])),
            "email": parse_email(data.get("emails", [])),
            "address": parse_address(data),
            "city": data.get("city"),
            "country": data.get("country"),
            "postal_code": str(data["postal_code"]) if data.get("postal_code") else None,
            "email_optin_flag": (data.get('unsubscribe_all_date') is None or data.get('unsubscribe_all_date') == '') and
                                (data.get('unsubscribe_email_date') is None or data.get('unsubscribe_email_date') == ''),
            "sms_optin_flag": (data.get('unsubscribe_all_date') is None or data.get('unsubscribe_all_date') == '') and
                              (data.get('unsubscribe_sms_date') is None or data.get('unsubscribe_sms_date') == '')
        }

        db_consumer = {key: value for key, value in db_consumer.items() if value}

        lead_status = data.get("status", "")

        # if status is not provided, then default value is Active. Activix considers 'Active' as having no status, whereas invalid, duplicate, lost are considered as having a status.
        if not lead_status:
            lead_status = "Active"

        db_lead = {
            "lead_ts": datetime.fromisoformat(data.get("created_at", datetime.utcnow().isoformat())).strftime("%Y-%m-%d %H:%M:%S.%f +0000"),
            "crm_lead_id": crm_lead_id,
            "lead_status": lead_status,
            "lead_substatus": "",
            "lead_comment": data.get("comment", "")[:5000],
            "lead_origin": data.get("type", ""),
            "lead_source": data.get("source", ""),
            "lead_source_detail": data.get("provider", "")
        }

        vehicles = data.get("vehicles", [])
        db_vehicles = []
        for vehicle in vehicles:
            db_vehicle = {}
            if vehicle.get("type") == "wanted":
                db_vehicle = {
                    "vin": vehicle.get("vin"),
                    "crm_vehicle_id": str(vehicle.get("id")),
                    "stock_num": vehicle.get("stock"),
                    "mileage": vehicle.get("odometer"),
                    "make": vehicle.get("make"),
                    "model": vehicle.get("model"),
                    "year": int(vehicle.get('year')) if vehicle.get('year') and vehicle.get('year').isdigit() else None,
                    "transmission": vehicle.get("transmission"),
                    "interior_color": vehicle.get("color_interior"),
                    "exterior_color": vehicle.get("color_exterior"),
                    "trim": vehicle.get("trim"),
                    "price": vehicle.get("price"),
                    "status": vehicle.get("type"),
                    "vehicle_comments": vehicle.get("comment")
                }
                # Ensure "wanted" vehicle is the first in the list
                db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}
                db_vehicles.insert(0, db_vehicle)
            elif vehicle.get("type") == "exchange":
                db_vehicle = {
                    "trade_in_vin": vehicle.get("vin"),
                    "trade_in_year": int(vehicle.get('year')) if vehicle.get('year') and vehicle.get('year').isdigit() else None,
                    "trade_in_make": vehicle.get("make"),
                    "trade_in_model": vehicle.get("model"),
                    "status": vehicle.get("type")
                }
                db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}
                db_vehicles.append(db_vehicle)
            else:
                logger.warning(f"Unhandled vehicle type: {vehicle.get('type')}")

        if data.get("bdc"):
            bdc = data.get("bdc")
            db_salesperson = extract_salesperson("BDC", bdc)
        elif data.get("advisor"):
            logger.info("No BDC rep found. Using advisor as salesperson.")
            advisor = data.get("advisor")
            db_salesperson = extract_salesperson("Advisor", advisor)
        else:
            db_salesperson = {}
            logger.info("No salesperson found in the lead data")

        parsed_data = {
            "product_dealer_id": product_dealer_id,
            "lead": db_lead,
            "consumer": db_consumer,
            "vehicles": db_vehicles,
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

        crm_lead_id = str(json_data["id"])
        crm_dealer_id = str(json_data["account_id"])

        parsed_lead = parse_lead(product_dealer_id, json_data)
        logger.info(f"Transformed record body: {parsed_lead}")

        consumer = parsed_lead["consumer"]
        if consumer.get("email") is None and consumer.get("phone") is None:
            logger.warning(f"Email or phone number is required. Ignoring lead {crm_lead_id}")
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
        logger.error(f"Error transforming activix record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, crm_lead_id, e)
            )
        raise


def lambda_handler(event, context):
    """Transform raw activix data to the unified format."""
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
