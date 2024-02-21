"""Update reyrey lead in the Impel CRM persistence layer."""
import json
from json import loads
import logging
import os
import boto3
from os import environ
from typing import Any, Dict
import xml.etree.ElementTree as ET
import requests
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
SECRET_KEY = environ.get("SECRET_KEY")

sm_client = boto3.client('secretsmanager')
s3_client = boto3.client("s3")


def make_crm_api_request(url: str, method: str, crm_api_key: str, data=None) -> Any:
    """Generic helper function to make CRM API requests."""

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    response = requests.request(method, url, headers=headers, json=data)
    logger.info(f"CRM API responded with: {response.status_code}")
    return response


def get_mapped_status(event_id: str, partner_name: str) -> Any:
    """Get mapped lead status from S3."""
    s3_key = f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
    try:
        s3_object = json.loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
        lead_updates = s3_object.get("lead_updates")
        lead_status = lead_updates.get(event_id)
    except Exception as e:
        logger.error(f"Failed to retrieve lead status from S3 config. Partner: {partner_name.upper()}, {e}")
        raise
    return lead_status


def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data


def get_lead(crm_lead_id: str, crm_dealer_id: str, crm_consumer_id: str, crm_api_key: str) -> Any:
    """Get lead by crm lead id through CRM API."""
    queryStringParameters = f"crm_dealer_id={crm_dealer_id}&integration_partner_name={SECRET_KEY}"

    url = f'https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}?{queryStringParameters}'

    response = make_crm_api_request(url, "GET", crm_api_key)
    response.raise_for_status()

    response_json = response.json()
    lead_id = response_json.get('lead_id')
    consumer_id = response_json.get('consumer_id')
    return lead_id, consumer_id


def update_lead_status(lead_id: str, data: dict, crm_api_key: str) -> Any:
    """Update lead status through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}'

    response = make_crm_api_request(url, "PUT", crm_api_key, data)
    response.raise_for_status()


def process_salespersons(response_data, new_salesperson):
    """Process salespersons from CRM API response."""
    try:
        new_first_name, new_last_name = new_salesperson.split()
    except ValueError:
        logger.warning(f"Salesperson name is not in the correct format: {new_salesperson}")
        new_first_name = new_salesperson.strip().replace(" ", "")
        new_last_name = ""

    crm_salesperson_id = new_salesperson.strip().replace(" ", "")
    logger.info(f"New Salesperson: {crm_salesperson_id}")

    if not response_data:
        logger.info("No salespersons found for this lead.")
        return [create_or_update_salesperson(new_first_name, new_last_name, crm_salesperson_id)]

    return [
        create_or_update_salesperson(new_salesperson)
        if salesperson.get('is_primary') and (salesperson.get('first_name') != new_first_name or salesperson.get('last_name') != new_last_name)
        else salesperson for salesperson in response_data
    ]


def create_or_update_salesperson(first_name, last_name, crm_salesperson_id):
    return {
        "crm_salesperson_id": crm_salesperson_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": "",
        "phone": "",
        "position_name": "Primary Salesperson",
        "is_primary": True
    }


def update_lead_salespersons(new_salesperson: str, lead_id: str, crm_api_key: str) -> Any:
    """Update lead salespersons through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}/salespersons'

    response = make_crm_api_request(url, "GET", crm_api_key)
    response.raise_for_status(response)

    salespersons = process_salespersons(response.json(), new_salesperson)
    return salespersons


def get_current_crm_consumer_id(crm_consumer_id: str, consumer_id: str, crm_api_key: str) -> Any:
    """Get CRM Consumer ID through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/consumers/{consumer_id}'

    response = make_crm_api_request(url, "GET", crm_api_key)
    if response.status_code != 200:
        error_msg = f"Error during fetching consumer: {response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)

    current_crm_consumer_id = response.json().get('crm_consumer_id')
    return current_crm_consumer_id


def get_existing_consumer_by_id(crm_consumer_id: str, crm_dealer_id: str, crm_api_key: str) -> Any:
    """Get existing consumer by CRM Consumer ID through CRM API."""
    url = f"https://{CRM_API_DOMAIN}/consumers/crm/{crm_consumer_id}?crm_dealer_id={crm_dealer_id}?integration_partner_name={SECRET_KEY}"

    response = make_crm_api_request(url, "GET", crm_api_key)

    if response.status_code == 404:
        logger.info(f"Existing consumer with CRM Consumer ID {crm_consumer_id} not found. {response.text}")
        return None

    response.raise_for_status()

    response_json = response.json()
    logger.info(f"Existing consumer with CRM Consumer ID {response_json}")
    consumer_id = response_json["consumer_id"]

    return consumer_id


def set_crm_consumer_id(crm_consumer_id: str, consumer_id: str, crm_api_key: str) -> None:
    """Set CRM Consumer ID through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/consumers/{consumer_id}'
    data = {
        "crm_consumer_id": crm_consumer_id
    }

    response = make_crm_api_request(url, "PUT", crm_api_key, data)
    response.raise_for_status()
    return


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record['body'])
        bucket = body["detail"]["bucket"]["name"]
        key = body["detail"]["object"]["key"]
        product_dealer_id = key.split('/')[2]

        logger.info(f"Product dealer id: {product_dealer_id}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        logger.info(f"XML Content: {content}")

        # Parse the XML content
        root = ET.fromstring(content)

        ns = {'ns': 'http://www.starstandards.org/STAR'}
        ET.register_namespace('', ns['ns'])

        application_area = root.find(".//ns:ApplicationArea", namespaces=ns)
        record = root.find(".//ns:Record", namespaces=ns)  # type: ignore

        dealer_number = None
        store_number = None
        area_number = None
        if application_area is not None:
            sender = application_area.find(".//ns:Sender", namespaces=ns)
            if sender is not None:
                dealer_number = sender.find(".//ns:DealerNumber", namespaces=ns).text  # type: ignore
                store_number = sender.find(".//ns:StoreNumber", namespaces=ns).text  # type: ignore
                area_number = sender.find(".//ns:AreaNumber", namespaces=ns).text  # type: ignore

        crm_dealer_id = f"{store_number}_{area_number}_{dealer_number}"
        logger.info(f"CRM Dealer ID: {crm_dealer_id}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        identifier = record.find(".//ns:Identifier", namespaces=ns)  # type: ignore
        crm_lead_id = identifier.find(".//ns:ProspectId", namespaces=ns).text
        logger.info(f"CRM Lead ID: {crm_lead_id}")

        crm_consumer_id = identifier.find(".//ns:NameRecId", namespaces=ns)
        if crm_consumer_id is not None:
            crm_consumer_id = crm_consumer_id.text

        event_id = record.find(".//ns:RCIDispositionEventId", namespaces=ns).text  # type: ignore
        event_name = record.find(".//ns:RCIDispositionEventName", namespaces=ns).text  # type: ignore
        logger.info(f"Event ID: {event_id}")
        logger.info(f"Event Name: {event_name}")

        lead_id, consumer_id = get_lead(crm_lead_id, crm_dealer_id, crm_consumer_id, crm_api_key)
        logger.info(f"Lead ID: {lead_id}")

        data: Dict[str, Any] = {}

        # Handle lead merge event
        if event_id == "19":
            merged_prospect_id = record.find(".//ns:RCIDispositionMergedProspectId", namespaces=ns).text  # type: ignore
            if not merged_prospect_id:
                raise Exception("Lead Merged User Event: MergedProspectId not provided.")

            data.update({
                "crm_lead_id": str(merged_prospect_id)
            })

        if crm_consumer_id:
            current_crm_consumer_id = get_current_crm_consumer_id(crm_consumer_id, consumer_id, crm_api_key)

            if current_crm_consumer_id and current_crm_consumer_id != crm_consumer_id:
                raise Exception(f"Consumer ID {consumer_id} is already associated with CRM Consumer ID {current_crm_consumer_id}.")

            if not current_crm_consumer_id:
                existing_consumer_id = get_existing_consumer_by_id(crm_consumer_id, crm_dealer_id, crm_api_key)

                # Reassign lead to existing consumer
                if existing_consumer_id:
                    logger.info(f"Existing consumer with CRM Consumer ID {crm_consumer_id} found. Reassigning lead to new consumer.")
                    data.update({
                        "consumer_id": existing_consumer_id
                    })
                # Set CRM Consumer ID for consumer
                else:
                    set_crm_consumer_id(crm_consumer_id, consumer_id, crm_api_key)

        mapped_lead_status = get_mapped_status(event_id=str(event_id), partner_name=SECRET_KEY)  # type: ignore
        data.update({
            'lead_status': mapped_lead_status,
            'metadata': {"updatedCrmLeadStatus": event_name}
        })

        salespersons = {}
        # update salesperson data if new salesperson is assigned to the lead
        if event_id == "30":
            new_salesperson = record.find(".//ns:RCIDispositionPrimarySalesperson", namespaces=ns)  # type: ignore
            if new_salesperson is not None:
                new_salesperson = new_salesperson.text  # type: ignore
            else:
                raise Exception("Field with salesperson name is not provided.")

            salespersons = update_lead_salespersons(new_salesperson, lead_id, crm_api_key)
            data['salespersons'] = salespersons

        update_lead_status(lead_id, data, crm_api_key)

    except Exception as e:
        logger.error(f"Error transforming reyrey lead update record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] ProductDealerId: {}\nLeadId: {}\nCrmDealerId: {}\nCrmLeadId: {}\nTraceback: {}".format(
            product_dealer_id, lead_id, crm_dealer_id, crm_lead_id, e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw reyrey lead update data to the unified format."""
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
