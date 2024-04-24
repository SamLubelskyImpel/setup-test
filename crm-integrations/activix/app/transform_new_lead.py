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


def parse_phone_number(phones):
    """Parse the phone number."""
    if not phones:
        return None

    preferred_phone = phones[0]
    for phone in phones:
        if phone.get('type') == 'mobile' and phone.get('valid') is True:
            preferred_phone = phone
            break 

    return preferred_phone


def parse_email(emails):
    """Parse the email."""
    if not emails:
        return None

    preferred_email = emails[0]

    for email in emails:
        if email.get('valid') is True:
            preferred_email = email
            break  

    return preferred_email


def parse_address(data):
    """Parse the address."""
    add1 = data.get("address_line1")
    add2 = data.get("address_line2")
    if add1 and add2:
        return f"{add1} {add2}"
    else:
        return add1 or add2 or None


def parse_lead(product_dealer_id, data):
    """Parse the JSON lead data."""
    parsed_data = {}
    try:
        crm_lead_id = data["id"]

        db_consumer = {
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "phone": parse_phone_number(data.get("phones", [])),
            "email": parse_email(data.get("emails", [])),
            "address": parse_address(data),
            "city": data.get("city"),
            "country": data.get("country"),
            "postal_code": str(data.get("postal_code", "")) if data.get("postal_code") else None,
            "email_optin_flag": user_data.get('unsubscribe_all_date') is None and user_data.get('unsubscribe_email_date') is None,
            "sms_optin_flag": user_data.get('unsubscribe_all_date') is None and user_data.get('unsubscribe_sms_date') is None
        }

        if not db_consumer["email"] and not db_consumer["phone"]:
            raise Exception("Email or phone number is required")

        db_consumer = {key: value for key, value in db_consumer.items() if value}

        db_lead = {
            "lead_ts": datetime.fromisoformat(data.get("created_at", datetime.utcnow().isoformat())).strftime("%Y-%m-%d %H:%M:%S.%f +0000"),
            "crm_lead_id": crm_lead_id,
            "lead_status": data.get("status", ""),
            "lead_substatus": "",
            "lead_comment": data.get("comment", ""),
            "lead_origin": data.get("type", ""),
            "lead_source": data.get("source", ""),
            "lead_source_detail": data.get("provider", "")
        }

        # vehicleType = data.get("vehicleType")
        # if vehicleType:
        #     vehicleType = vehicleType.lower().capitalize()
        #     if vehicleType not in ("New", "Used"):
        #         logger.warning(f"Unexpected vehicle type: {vehicleType}")

        # db_vehicle = {
        #     "vin": data.get("vin"),
        #     "stock_num": data.get("stock"),
        #     "make": data.get("make"),
        #     "model": data.get("model"),
        #     "year": data.get("year"),
        #     "exterior_color": data.get("color"),
        #     "trim": data.get("trim"),
        #     "condition": vehicleType
        # }
        # db_vehicle = {key: value for key, value in db_vehicle.items() if value is not None}

        # if data.get("bdcID"):
        #     db_salesperson = extract_salesperson(data["bdcID"], data.get("bdcName"), "BDC Rep")

        # elif data.get("contactID"):
        #     logger.info("No BDC rep found. Using sales rep as salesperson.")
        #     db_salesperson = extract_salesperson(data["contactID"], data.get("contactName"))

        # else:
        #     db_salesperson = {}
        #     logger.info("No salesperson found in the lead data")

        # parsed_data = {
        #     "product_dealer_id": product_dealer_id,
        #     "lead": db_lead,
        #     "consumer": db_consumer,
        #     "vehicle": db_vehicle,
        #     "salesperson": db_salesperson
        # }

        # return parsed_data

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
        crm_dealer_id = json_data["account_id"]

        parsed_lead = parse_lead(product_dealer_id, json_data)
        logger.info(f"Transformed record body: {parsed_lead}")
        # if parsed_lead["lead"]["lead_origin"] != "Internet":
        #     logger.warning(f"Lead type is not Internet: {parsed_lead['lead']['lead_origin']}. Ignoring lead.")
        #     return

        # crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]
        # existing_lead = get_existing_lead(crm_lead_id, crm_dealer_id, crm_api_key)
        # if existing_lead:
        #     logger.warning(f"Existing lead detected: DB Lead ID {existing_lead}. Ignoring duplicate lead.")
        #     return

        # consumer_id = create_consumer(parsed_lead, crm_api_key)["consumer_id"]
        # lead_id = create_lead(parsed_lead, consumer_id, crm_api_key)["lead_id"]

        # logger.info(f"New lead created: {lead_id}")

    except Exception as e:
        logger.error(f"Error transforming momentum record - {record}: {e}")
        # logger.error("[SUPPORT ALERT] Failed to Transform New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
        #     product_dealer_id, crm_dealer_id, crm_lead_id, e)
        #     )
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
