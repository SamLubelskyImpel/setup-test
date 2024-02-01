"""Transform raw ReyRey_CRM data into the Unified Format."""

import boto3
import logging
import requests
import json
import uuid
import xml.etree.ElementTree as ET
from os import environ
from typing import Any, Dict
from datetime import datetime
from utils import send_email_notification
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    SqsFifoPartialProcessor,
    process_partial_response,
)


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
PARTNER_ID = environ.get("PARTNER_ID")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
DA_SECRET_KEY = environ.get("DA_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

sm_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


class EventListenerError(Exception):
    pass

class LeadExistsException(Exception):
    pass

class ConsumerCreationException(Exception):
    pass

class LeadCreationException(Exception):
    pass


def send_to_event_listener(lead_id: int, listener_secrets: dict) -> None:
    """Send notification to DA Event listener."""
    try:
        data = {
            "message": "New Lead available from CRM API",
            "lead_id": lead_id,
        }
        response = requests.post(
            url=listener_secrets["API_URL"],
            headers={"Authorization": listener_secrets["API_TOKEN"]},
            json=data,
            timeout=30,
        )
        logger.info(f"DA Event Listener responded with status: {response.status_code}")
        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout occurred calling DA Event Listener for the lead {lead_id}"
        )
    except Exception as e:
        logger.error(f"Error occurred calling DA Event Listener: {e}")
        raise EventListenerError


def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data


def get_text(element, path, namespace):
    """Get the text of the element if it's present."""
    found_element = element.find(path, namespace)
    return found_element.text if found_element is not None else None


def extract_consumer(root: ET.Element, namespace: dict) -> dict:
    """Extract consumer data from the XML."""
    name_rec_id = get_text(root, ".//star:NameRecId", namespace)
    first_name = root.find(".//star:FirstName", namespace).text
    last_name = root.find(".//star:LastName", namespace).text
    email_mail_to = root.find(".//star:Email/star:MailTo", namespace).text
    phone_num = root.find(".//star:PhoneNumbers/star:Phone/star:Num", namespace).text
    consent_email = root.find(".//star:Consent/star:Email", namespace).text
    consent_text = root.find(".//star:Consent/star:Text", namespace).text

    # Assemble the payload for the CRM API
    extracted_data = {
        "crm_consumer_id": name_rec_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email_mail_to,
        "phone": phone_num,
        "email_optin_flag": False if consent_email == "N" else True,
        "sms_optin_flag": False if consent_text == "N" else True,
    }
    return extracted_data


def extract_lead(root: ET.Element, namespace: dict) -> dict:
    """Extract lead, vehicle of interest, salesperson data from the XML."""

    def convert_time_format(original_time):
        """Convert the time format from 'Lead 2024-01-18T10:56:59' to '2021-06-16T13:44:00Z'"""

        # IMPORTANT
        # Previous provided example had the 'Last, First 6/16/2021 1:44 PM' time format, in the new example the time format is 'Lead 2024-01-18T10:56:59'
        # Commenting out the previous parser and adding the new one to support the new format

        # # Split the name and date/time parts
        # print(f"\n\n{original_time}\n\n")
        # first_name, last_name, date_time_str = original_time.split(" ", 2)

        # # Parse the date and time into a datetime object
        # date_time_obj = datetime.strptime(date_time_str, "%m/%d/%Y %I:%M %p")

        # # Format the datetime object into the desired format
        # formatted_time = date_time_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

        formatted_time = original_time.split(" ")[1] + "Z"

        return formatted_time

    def map_status(status: str) -> str:
        """Map the initial ReyRey status to the Unified Layer status."""
        response = s3_client.get_object(
            Bucket=INTEGRATIONS_BUCKET,
            Key=f"configurations/{ENVIRONMENT}_{SECRET_KEY.upper()}.json",
        )
        config = json.loads(response["Body"].read())
        status_map = config["initial_status_map"]
        unified_layer_status = status_map.get(status, None)

        if not unified_layer_status:
            logger.error(
                f"Error mapping status: {status}, status not found in the status map"
            )

        return unified_layer_status

    # Extract Prospect fields
    prospect_id = root.find(".//star:ProspectId", namespace).text
    inserted_by = root.find(".//star:InsertedBy", namespace).text
    prospect_status_type = root.find(".//star:ProspectStatusType", namespace).text
    prospect_note = root.find(".//star:ProspectNote", namespace).text
    prospect_type = root.find(".//star:ProspectType", namespace).text

    prospect_data = {
        "crm_lead_id": prospect_id,
        "lead_ts": convert_time_format(inserted_by),
        "lead_status": map_status(prospect_status_type),
        "lead_substatus": None,
        "lead_comment": prospect_note,
        "lead_origin": prospect_type,
        "lead_source": None,
    }

    # Extract Vehicle of Interest fields
    vin = root.find(".//star:DesiredVehicle/star:Vin", namespace).text
    stock_id = root.find(".//star:DesiredVehicle/star:StockId", namespace)
    stock_id = stock_id.text if stock_id is not None else None
    vehicle_make = root.find(".//star:DesiredVehicle/star:VehicleMake", namespace).text
    vehicle_model = root.find(
        ".//star:DesiredVehicle/star:VehicleModel", namespace
    ).text
    vehicle_year = root.find(".//star:DesiredVehicle/star:VehicleYear", namespace).text
    vehicle_style = root.find(
        ".//star:DesiredVehicle/star:VehicleStyle", namespace
    ).text
    stock_type = root.find(".//star:DesiredVehicle/star:StockType", namespace).text

    vehicle_of_interest_data = {
        "vin": vin,
        "stock_number": stock_id,
        "make": vehicle_make,
        "oem_name": vehicle_make,
        "model": vehicle_model,
        "year": vehicle_year,
        "type": vehicle_style,
        "body_style": vehicle_style,
        "condition": stock_type,
        "class": None,
        "mileage": None,
        "trim": None,
        "transmission": None,
        "interior_color": None,
        "exterior_color": None,
        "price": None,
        "status": None,
        "odometer_units": None,
        "vehicle_comments": None,
    }

    # Extract Salesperson fields, primary salesperson format: "Last, First"
    primary_salesperson = root.find(
        ".//star:Record/star:Prospect/star:PrimarySalesPerson", namespace
    )
    salesperson_data = None
    if primary_salesperson is not None:
        primary_salesperson = primary_salesperson.text
        first_name = primary_salesperson.split(",")[1].strip()
        last_name = primary_salesperson.split(",")[0].strip()
        
        impel_salesperson_id = f"Impel_generated_{str(uuid.uuid4())}"

        salesperson_data = {
            "crm_salesperson_id": impel_salesperson_id,
            "first_name": first_name,
            "last_name": last_name,
            "is_primary": True,
            "position_name": "Primary Salesperson",
            "email": None,
            "phone": None,
        }

    # Add Vehicle of Interest and Salesperson data to the Prospect data
    prospect_data["vehicles_of_interest"] = [vehicle_of_interest_data]
    prospect_data["salespersons"] = [salesperson_data] if salesperson_data else []

    return prospect_data


def get_lead(crm_lead_id: str, crm_dealer_id: str, crm_api_key: str) -> Any:
    """Check if lead exists through CRM API."""
    queryStringParameters = f"crm_dealer_id={crm_dealer_id}&integration_partner_name={SECRET_KEY}"
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
    else:
        logger.error(f"Error getting lead with crm_lead_id {crm_lead_id}: {response.text}")
        raise


def get_crm_dealer_id(root: ET.Element, ns: Any) -> str:
    application_area = root.find(".//star:ApplicationArea", namespaces=ns)

    dealer_number = None
    store_number = None
    area_number = None
    if application_area is not None:
        sender = application_area.find(".//star:Sender", namespaces=ns)
        if sender is not None:
            dealer_number = sender.find(".//star:DealerNumber", namespaces=ns).text
            store_number = sender.find(".//star:StoreNumber", namespaces=ns).text
            area_number = sender.find(".//star:AreaNumber", namespaces=ns).text

    crm_dealer_id = f"{store_number}_{area_number}_{dealer_number}"
    return crm_dealer_id


def create_consumer_in_unified_layer(consumer: dict, lead: dict, root: ET.Element, namespace: dict, product_dealer_id: str, crm_api_key: str) -> Any:
    """Create a new consumer in the Unified Layer."""

    # If the CRM consumer ID is not present, check whether the lead exists in the Unified Layer. If it does, throw an error; otherwise, create a new consumer.
    if consumer["crm_consumer_id"] is None:
        crm_lead_id = lead["crm_lead_id"]
        crm_dealer_id = get_crm_dealer_id(root, namespace)
        lead = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)
        if lead:
            logger.error(f"Lead with crm_lead_id {crm_lead_id} already exists.")
            raise LeadExistsException(f"Lead with crm_lead_id {crm_lead_id} already exists.")

    logger.info(f"Consumer data to send: {consumer}")
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/consumers?dealer_id={product_dealer_id}",
        json=consumer,
        headers={
            "x_api_key": crm_api_key,
            "partner_id": UPLOAD_SECRET_KEY,
        },
    )
    logger.info(
        f"Response from Unified Layer Create Customer {response.status_code} {response.text}",
    )

    unified_crm_consumer_id = response.json().get("consumer_id")

    if not unified_crm_consumer_id:
        logger.error(f"Error creating the consumer: {consumer}")
        raise ConsumerCreationException(f"Error creating consumer: {consumer}")

    return unified_crm_consumer_id


def create_lead_in_unified_layer(lead: dict[Any, Any], crm_api_key: str, product_dealer_id: str) -> Any:
    """Create a new lead in the Unified Layer."""
    response = requests.post(
        f"https://{CRM_API_DOMAIN}/leads",
        json=lead,
        headers={"x_api_key": crm_api_key, "partner_id": UPLOAD_SECRET_KEY},
    )
    logger.info(
        f"Response from Unified Layer Create Lead {response.status_code} {response.text}"
    )

    unified_crm_lead_id = response.json().get("lead_id")

    if not unified_crm_lead_id:
        logger.error(f"Error creating lead: {lead}")
        raise LeadCreationException(f"Error creating lead: {lead}")

    return unified_crm_lead_id


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        message = json.loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]
        product_dealer_id = key.split("/")[2]
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read()
        xml_data = content
        logger.info(f"Raw data: {xml_data}")
        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)[
            "api_key"
        ]

        root = ET.fromstring(xml_data)
        namespace = {"star": "http://www.starstandards.org/STAR"}

        consumer = extract_consumer(root, namespace)
        lead = extract_lead(root, namespace)
        unified_crm_consumer_id = create_consumer_in_unified_layer(consumer, lead, root, namespace, product_dealer_id, crm_api_key)

        # Write new lead to the Unified Layer, using the consumer_id that was just created
        lead["consumer_id"] = unified_crm_consumer_id
        logger.info(f"Lead data to send: {lead}")

        unified_crm_lead_id = create_lead_in_unified_layer(lead, crm_api_key, product_dealer_id)

        event_listener_secrets = get_secret(
            secret_name="crm-integrations-partner", secret_key=DA_SECRET_KEY
        )

        send_to_event_listener(unified_crm_lead_id, event_listener_secrets)
        logger.info(f"Successfully sent the lead {unified_crm_lead_id} to DA")
    except EventListenerError:
        message = f"Error sending the lead {unified_crm_lead_id} to DA"
        logger.error(message)
        send_email_notification(message)
        raise
    except ConsumerCreationException:
        raise
    except LeadExistsException:
        raise
    except LeadCreationException:
        raise
    except Exception as e:
        logger.error(f"Error transforming ReyRey record - {record}: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw ReyRey_CRM data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        processor = SqsFifoPartialProcessor()
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
        return result

    except Exception as e:
        logger.error(f"Error processing ReyRey new lead: {e}")
        raise
