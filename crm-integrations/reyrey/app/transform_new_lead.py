"""Transform raw ReyRey_CRM data into the Unified Format."""

import boto3
import logging
import requests
import json
import re
import xml.etree.ElementTree as ET
from os import environ
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
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
PARTNER_ID = environ.get("PARTNER_ID")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

sm_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


class LeadExistsException(Exception):
    pass

class ConsumerCreationException(Exception):
    pass

class LeadCreationException(Exception):
    pass

class NotInternetLeadException(Exception):
    pass

class NoCustomerInitiatedLeadException(Exception):
    pass

class CustomerContactInfoError(Exception):
    pass

def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data


def get_text(element: Any, path: str, namespace: Any) -> Any:
    """Get the text of the element if it's present."""
    found_element = element.find(path, namespace)
    if found_element is not None and found_element.text != "Null":
        return found_element.text
    else:
        return None


def extract_phone(root: ET.Element, namespace: dict) -> str:
    """Extract phone number from the XML."""
    first_phone_num = get_text(root, ".//star:PhoneNumbers/star:Phone/star:Num", namespace)

    # Try to find a phone number with Type 'C' (Cell phone)
    phone_num_type_c = get_text(root, ".//star:PhoneNumbers/star:Phone[star:Type='C']/star:Num", namespace)

    # Use Type C number if found otherwise use the first phone number
    phone_num = phone_num_type_c if phone_num_type_c is not None else first_phone_num

    return phone_num


def extract_consumer(root: ET.Element, namespace: dict) -> dict:
    """Extract consumer data from the XML."""
    fields = {
        "crm_consumer_id": get_text(root, ".//star:NameRecId", namespace),
        "first_name": get_text(root, ".//star:FirstName", namespace),
        "last_name": get_text(root, ".//star:LastName", namespace),
        "email": get_text(root, ".//star:Email/star:MailTo", namespace),
        "phone": extract_phone(root, namespace),
        "email_optin_flag": False if get_text(root, ".//star:Consent/star:Email", namespace) == "N" else True,
        "sms_optin_flag": False if get_text(root, ".//star:Consent/star:Text", namespace) == "N" else True,
    }

    extracted_data = {}

    for key, value in fields.items():
        if value:
            extracted_data[key] = value

    if not extracted_data.get("email") and not extracted_data.get("phone"):
        raise CustomerContactInfoError(f"Consumer does not have an email or phone number.")

    return extracted_data


def update_non_internet_leads_config(lead_id: str, crm_dealer_id) -> None:
    """Updates or creates a configuration file to include a specified non-internet lead for a CRM dealer."""
    s3_key = f"configurations/reyrey_crm/ignore/{crm_dealer_id}.json"
    try:
        logger.info(f"Lead id: {lead_id}")
        try:
            response = s3_client.get_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key)
            existing_content = response['Body'].read().decode('utf-8')
            existing_data = json.loads(existing_content)
            non_internet_leads = set(existing_data.get("non_internet_leads", []))
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"No existing data for {crm_dealer_id}, creating a new list.")
            non_internet_leads = set()

        non_internet_leads.add(lead_id)
        updated_data = {"non_internet_leads": list(non_internet_leads)}
        updated_content = json.dumps(updated_data)
        s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=updated_content)
        logger.info(f"Successfully updated non internet leads for {crm_dealer_id}.")

    except Exception as e:
        logger.error(f"Failed to update S3 object. Partner: {SECRET_KEY}, Error: {str(e)}")
        raise


def extract_lead(root: ET.Element, namespace: dict, crm_dealer_id: str) -> dict:
    """Extract lead, vehicle of interest, salesperson data from the XML."""

    def extract_note(notes: list) -> str:
        """Extract note which contains actual lead comment."""
        if len(notes) > 0:
            for note in notes:
                if "Best Time" not in note.text:
                    return note.text
            # If no note without "Best Time" is found, return first note.
            return notes[0].text
        else:
            return ""

    def convert_time_format(original_time: str, metadata: dict) -> tuple[str, dict]:
        """
        Try to extract time from the XML if it is like this: 2023-12-31T12:00:00 otherwise use current time.
        If the current time has been used, add original time to the metadata.
        """
        pattern = r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\b"
        matches = re.findall(pattern, original_time)

        if len(matches) > 0:
            formatted_time = matches[0]
        else:
            current_time = datetime.now()
            formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
            metadata["original_lead_insert_time"] = original_time

        return formatted_time, metadata

    def map_status(status: str, metadata: dict) -> tuple[str, dict]:
        """Map the initial ReyRey status to the Unified Layer status."""
        response = s3_client.get_object(
            Bucket=INTEGRATIONS_BUCKET,
            Key=f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_{SECRET_KEY.upper()}.json",
        )
        config = json.loads(response["Body"].read())
        status_map = config["initial_status_map"]
        unified_layer_status = status_map.get(status, None)
        metadata["original_status"] = status

        if not unified_layer_status:
            logger.error(
                f"Error mapping status: {status}, status not found in the status map"
            )

        return unified_layer_status, metadata

    def extract_and_process_salesperson_data(root, xpath, role_name, is_primary=False, namespace=None):
        salesperson = root.find(xpath, namespace)
        if salesperson is not None:
            salesperson_name = salesperson.text.strip()

            try:
                last_name, first_name = [name.strip() for name in salesperson_name.split(",")]
                crm_salesperson_id = f"{first_name}{last_name}"
            except ValueError:
                logger.warning(f"Salesperson name is not in the correct format: {salesperson_name}")

                # Remove any commas, treat the entire name as the first name for non-standard formats
                first_name = salesperson_name.replace(",", "").strip()
                last_name = ""
                crm_salesperson_id = first_name.replace(" ", "")

            return {
                "crm_salesperson_id": crm_salesperson_id,
                "first_name": first_name,
                "last_name": last_name,
                "is_primary": is_primary,
                "position_name": role_name,
                "email": None,
                "phone": None,
            }

    # Extract Prospect fields
    prospect_id = root.find(".//star:ProspectId", namespace).text
    inserted_by = get_text(root, ".//star:InsertedBy", namespace)
    prospect_status_type = root.find(".//star:ProspectStatusType", namespace).text
    prospect_note = extract_note(root.findall(".//star:ProspectNote", namespace))
    prospect_type = get_text(root, ".//star:ProspectType", namespace)
    provider_name = get_text(root, ".//star:ProviderName", namespace)
    is_ci_lead = get_text(root, ".//star:IsCiLead", namespace)
    prospect_source_detail = get_text(root, ".//star:ProviderService", namespace)

    if is_ci_lead == "false":
        raise NoCustomerInitiatedLeadException(f"Lead is not customer initiated: {prospect_id}")

    if prospect_type != "Internet":
        update_non_internet_leads_config(prospect_id, crm_dealer_id)
        raise NotInternetLeadException(f"Lead type is not Internet: {prospect_type}")

    metadata = {}

    lead_ts, metadata = convert_time_format(inserted_by, metadata)
    lead_status, metadata = map_status(prospect_status_type, metadata)

    prospect_data = {
        "crm_lead_id": prospect_id,
        "lead_ts": lead_ts,
        "lead_status": lead_status,
        "lead_substatus": None,
        "lead_comment": prospect_note,
        "lead_origin": prospect_type,
        "lead_source": provider_name,
        "lead_source_detail": prospect_source_detail
    }

    # Extract Vehicle of Interest fields
    vin = get_text(root, ".//star:DesiredVehicle/star:Vin", namespace)
    stock_id = get_text(root, ".//star:DesiredVehicle/star:StockId", namespace)
    vehicle_make = get_text(root, ".//star:DesiredVehicle/star:VehicleMake", namespace)
    vehicle_model = get_text(root, ".//star:DesiredVehicle/star:VehicleModel", namespace)
    vehicle_year = get_text(root, ".//star:DesiredVehicle/star:VehicleYear", namespace)
    vehicle_style = get_text(root, ".//star:DesiredVehicle/star:VehicleStyle", namespace)
    stock_type = get_text(root, ".//star:DesiredVehicle/star:StockType", namespace)
    trade_in_vin = get_text(root, ".//star:PotentialTrade/star:TradeVehicleVin", namespace)
    trade_in_year = get_text(root, ".//star:PotentialTrade/star:TradeVehicleYear", namespace)
    trade_in_make = get_text(root, ".//star:PotentialTrade/star:TradeVehicleMake", namespace)
    trade_in_model = get_text(root, ".//star:PotentialTrade/star:TradeVehicleModel", namespace)

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
        "trade_in_vin": trade_in_vin,
        "trade_in_year": trade_in_year,
        "trade_in_make": trade_in_make,
        "trade_in_model": trade_in_model
        # "class": None,
        # "mileage": None,
        # "trim": None,
        # "transmission": None,
        # "interior_color": None,
        # "exterior_color": None,
        # "price": None,
        # "status": None,
        # "odometer_units": None,
        # "vehicle_comments": None,
    }

    salespersons_data = []

    salespersons_roles = [
        (".//star:Record/star:Prospect/star:PrimarySalesPerson", "Primary Salesperson", True),
        (".//star:Record/star:Prospect/star:Manager", "Manager"),
        (".//star:Record/star:Prospect/star:BDCUser", "BDC User"),
    ]

    for xpath, role_name, *is_primary in salespersons_roles:
        salesperson_data = extract_and_process_salesperson_data(root, xpath, role_name, bool(is_primary), namespace)
        if salesperson_data:
            salespersons_data.append(salesperson_data)

    # Add Vehicle of Interest and Salesperson data to the Prospect data
    prospect_data["vehicles_of_interest"] = [vehicle_of_interest_data]
    prospect_data["salespersons"] = salespersons_data if salespersons_data else []
    prospect_data["metadata"] = metadata

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


def create_consumer_in_unified_layer(consumer: dict, lead: dict, root: ET.Element, namespace: dict, crm_dealer_id: str, product_dealer_id: str, crm_api_key: str) -> Any:
    """Create a new consumer in the Unified Layer."""

    # If the CRM consumer ID is not present, check whether the lead exists in the Unified Layer. If it does, throw an error; otherwise, create a new consumer.
    if not consumer.get("crm_consumer_id"):
        crm_lead_id = lead["crm_lead_id"]
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
    logger.info(f"Lead data to send: {lead}")
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

        crm_dealer_id = get_crm_dealer_id(root, namespace)
        consumer = extract_consumer(root, namespace)
        lead = extract_lead(root, namespace, crm_dealer_id)
        unified_crm_consumer_id = create_consumer_in_unified_layer(consumer, lead, root, namespace, crm_dealer_id, product_dealer_id, crm_api_key)

        lead["consumer_id"] = unified_crm_consumer_id

        unified_crm_lead_id = create_lead_in_unified_layer(lead, crm_api_key, product_dealer_id)
        logger.info(f"Lead successfully created: {unified_crm_lead_id}")
    except ConsumerCreationException:
        raise
    except LeadExistsException:
        raise
    except LeadCreationException:
        raise
    except NotInternetLeadException:
        logger.info("Lead type is not Internet")
    except NoCustomerInitiatedLeadException:
        logger.info("Lead is not customer initiated")
    except Exception as e:
        logger.error(f"Error transforming ReyRey record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw ReyRey_CRM data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        # processor = BatchProcessor(event_type=EventType.SQS)
        record_handler(event)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except CustomerContactInfoError:
       logger.warning("Lead doesn't contain customer's email and phone")
       return
    except Exception as e:
        logger.error(f"Error processing ReyRey new lead: {e}")
        raise
