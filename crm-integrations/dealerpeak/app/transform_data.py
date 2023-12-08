"""Transform raw dealerpeak data to the unified format."""

import boto3
import logging
import requests
from os import environ
from json import loads, dumps
from typing import Any, Dict
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
DA_EVENT_LISTENER = environ.get("DA_EVENT_LISTENER")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

sm_client = boto3.client('secretsmanager')
s3_client = boto3.client("s3")


class WebhookError(Exception):
    pass


def get_secret() -> Any:
    """Get CRM API secret."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret["SecretString"])[str(UPLOAD_SECRET_KEY)]
    secret_data = loads(secret)

    return secret_data["api_key"]


def upload_entry_to_db(entry: Dict[str, Any]) -> Any:
    """Upload entries to the database through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/upload'
    api_key = get_secret()

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': api_key
    }

    try:
        response = requests.post(url, headers=headers, json=entry)
        logger.info(f"CRM API responded with status: {response.status_code}")
        response.raise_for_status()
        response_data = response.json()
        lead_id = response_data.get('lead_id')
        return lead_id

    except Exception as e:
        logger.error(f"Error occurred calling CRM API: {e}")
        raise


def format_ts(input_ts: str) -> str:
    """
    Format a timestamp string into a db format.

    Assumes the input timestamp is in the format "November, 17 2023 18:57:17".
    """
    dt = datetime.strptime(input_ts, "%B, %d %Y %H:%M:%S")

    # Format the datetime object to ISO 8601 UTC format
    output_format = "%Y-%m-%dT%H:%M:%SZ"
    return dt.strftime(output_format)


def extract_contact_information(item_name: str, item: Any, db_entity: Any) -> None:
    """Extract contact information from the dealerpeak json data."""
    db_entity[f'crm_{item_name}_id'] = item.get('userID')
    db_entity["first_name"] = item.get('givenName')
    db_entity["last_name"] = item.get('familyName')
    emails = item.get('contactInformation', {}).get('emails', [])
    db_entity["email"] = emails[0].get('address', '') if emails else ''
    phone_numbers = item.get('contactInformation', {}).get('phoneNumbers', [])
    phone_number = phone_numbers[0].get("number") if phone_numbers and "number" in phone_numbers[0] else ''

    # Iterate to find a mobile, cell or main phone number
    for phone in phone_numbers:
        if phone.get("type", "").lower() in ["mobile", "cell", "main"] and phone.get("number"):
            phone_number = phone.get('number', '')
            break

    if phone_number:
        db_entity["phone"] = phone_number

    if item_name == 'consumer':
        addresses = item.get('contactInformation', {}).get('addresses', [])
        address = addresses[0] if addresses else None

        if addresses:
            for addr in addresses:
                if addr.get("type", "").lower() == "main":
                    address = addr
                    break

        if address:
            line1 = address.get('line1', '')
            if not line1:
                line1 = address.get('lineOne', '')
            line2 = address.get('line2', '')
            db_entity["address"] = f"{line1} {line2}".strip() if line1 or line2 else None
            db_entity["city"] = address.get('city', None)
            db_entity["postal_code"] = address.get('postcode', None)

        communication_preferences = item.get('contactInformation', {}).get('allowed', {})

        if communication_preferences:
            db_entity["email_optin_flag"] = communication_preferences.get('email', True)


def remove_none_or_empty_entries(d, key=None):
    """Recursively remove None entries and empty string entries from a dictionary, except for the 'salesperson' key."""
    if isinstance(d, dict):
        return {k: remove_none_or_empty_entries(v, k) for k, v in d.items() if (v is not None and v != '') or (key == 'salesperson')}
    return d


def parse_json_to_entries(product_dealer_id: str, json_data: Any) -> Any:
    """Format dealerpeak json data to unified format."""
    entries = []
    try:
        for item in json_data:
            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            db_lead["crm_lead_id"] = item.get('leadID')
            db_lead["lead_ts"] = format_ts(item.get('dateCreated'))
            db_lead["lead_status"] = item.get('status', {}).get('status')
            db_lead["lead_comment"] = item.get('firstNote', {}).get('note')
            db_lead["lead_source"] = item.get('source', {}).get('source')

            vehicles = item.get('vehiclesOfInterest', [])
            for vehicle in vehicles:
                db_vehicle = {}
                db_vehicle["crm_vehicle_id"] = vehicle.get('carID')
                db_vehicle["vin"] = vehicle.get('vin')
                db_vehicle["year"] = int(vehicle.get('year'))
                db_vehicle["make"] = vehicle.get('make')
                db_vehicle["model"] = vehicle.get('model')

                is_new = vehicle.get("isNew")
                db_vehicle["condition"] = 'New' if is_new is True else ('Used' if is_new is False else None)

                db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            consumer = item.get('customer', None)
            extract_contact_information('consumer', consumer, db_consumer)

            salesperson = item.get('agent', None)
            extract_contact_information('salesperson', salesperson, db_salesperson)

            entry = {
                "product_dealer_id": product_dealer_id,
                "lead": db_lead,
                "consumer": db_consumer,
                "salesperson": db_salesperson,
            }

            cleaned_entry = remove_none_or_empty_entries(entry)
            entries.append(cleaned_entry)

        return entries
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def send_notification_to_webhook(data: Dict[str, Any]) -> None:
    """Send notification to Sales AI Webhook."""
    try:
        response = requests.post(DA_EVENT_LISTENER, json=data)
        logger.info(f"Sales AI Webhook responded with status: {response.status_code}")
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error occurred calling Sales AI Webhook: {e}")
        raise WebhookError


def send_alert_notification(e: Exception, lead_id: int) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred while sending data to Sales AI webhook: {e}",
        "lead_id": lead_id
    }
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps(data)}),
        Subject='Dealerpeak - Sales AI Webhook Failure Alert',
        MessageStructure='json'
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealerpeak data to the unified format."""
    logger.info(f"Event: {event}")
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            bucket = message["detail"]["bucket"]["name"]
            key = message["detail"]["object"]["key"]
            product_dealer_id = key.split('/')[2]

            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            json_data = loads(content)

            logger.info(f"Raw data: {json_data}")

            entries = parse_json_to_entries(product_dealer_id, json_data)

            logger.info(f"Processed entries: {entries}")

            for entry in entries:
                try:
                    lead_id = upload_entry_to_db(entry)
                    data = {
                        "message": "New Lead available from CRM API",
                        "lead_id": lead_id,
                    }
                    send_notification_to_webhook(data)
                except WebhookError as e:
                    send_alert_notification(e, lead_id)
                except Exception as e:
                    logger.error(f"Error uploading entry to DB: {e}")
    except Exception:
        logger.exception(f"Error transforming dealerpeak file {event}")
        raise
