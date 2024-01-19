import logging
import boto3
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from os import environ
from typing import Any
from uuid import uuid4

BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_URL = environ.get("CRM_API_URL")
ENVIRONMENT = environ.get("ENVIRONMENT")
PARTNER_ID = environ.get("PARTNER_ID")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")


def create_soap_response(error_message):
    # Create SOAP response for error
    envelope = ET.Element('soap:Envelope', xmlns="http://schemas.xmlsoap.org/soap/envelope/")
    body = ET.SubElement(envelope, 'soap:Body')
    fault = ET.SubElement(body, 'soap:Fault')
    fault_string = ET.SubElement(fault, 'faultstring')
    fault_string.text = error_message

    return ET.tostring(envelope, encoding="unicode")


def get_secrets():
    """Get CRM API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = json.loads(secret["SecretString"])[PARTNER_ID]
    secret_data = json.loads(secret)

    return secret_data["api_key"]


def get_dealers(integration_partner_name: str) -> Any:
    """Get dealers from CRM API."""
    api_key = get_secrets()
    url = f"{CRM_API_URL}dealers"

    response = requests.get(
        url=url,
        headers={"partner_id": PARTNER_ID, "x_api_key": api_key},
        params={"integration_partner_name": integration_partner_name},
    )
    logger.info(f"CRM API responded with: {response.status_code}")
    if response.status_code != 200:
        logger.error(
            f"Error getting dealers {integration_partner_name}: {response.text}"
        )
        raise

    return response.json()


def save_raw_lead(lead: str, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw_updates/reyrey/{product_dealer_id}/{date_key}_{uuid4()}.xml"
    logger.info(f"Saving reyrey lead to {s3_key}")
    s3_client.put_object(
        Body=lead,
        Bucket=BUCKET,
        Key=s3_key,
    )


def extract_crm_dealer_id(lead_xml_body: str) -> str:
    """Extract CRM dealer ID from incoming xml."""
    try:
        root = ET.fromstring(lead_xml_body)

        namespace = {"star": "http://www.starstandards.org/STAR"}

        dealer_number = root.find(".//star:DealerNumber", namespace).text
        store_number = root.find(".//star:StoreNumber", namespace).text
        area_number = root.find(".//star:AreaNumber", namespace).text

        concatenated_dealer_id = f"{store_number}_{area_number}_{dealer_number}"
        logger.info(f"Extracted CRM dealer ID: {concatenated_dealer_id}")
    except Exception as e:
        logger.error(f"Error parsing XML: {e}")
        raise

    return concatenated_dealer_id


def lambda_handler(event: Any, context: Any) -> Any:
    """This API handler takes the XML sent by ReyRey and puts the raw XML into the S3 bucket."""
    try:
        logger.info(f"Event: {event}")

        lead_xml_body = event["body"]
        reyrey_dealer_list = get_dealers("REYREY")
        crm_dealer_id = extract_crm_dealer_id(lead_xml_body)

        product_dealer_id = None
        for dealer in reyrey_dealer_list:
            if dealer["crm_dealer_id"] == crm_dealer_id:
                product_dealer_id = dealer["product_dealer_id"]
                break

        if not product_dealer_id:
            logger.error(f"Dealer {crm_dealer_id} not found in active dealers.")
            error_message = f"The dealer_id {crm_dealer_id} provided hasn't been configured with Impel."
            soap_response = create_soap_response(error_message)
            return {
                "statusCode": 401,
                "headers": {"Content-Type": "text/xml"},
                "body": soap_response
            }

        logger.info("New lead received for dealer: " + product_dealer_id)
        logger.info(f"Lead body: {lead_xml_body}")
        save_raw_lead(str(lead_xml_body), product_dealer_id)

        return {
            "statusCode": 200
        }

    except ValueError as e:
        soap_response = create_soap_response(str(e))
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "text/xml"},
            "body": soap_response
        }
    except Exception as e:
        logger.error(f"Error getting ReyRey lead update: {str(e)}")
        error_message = "Internal Server Error. Please contact Impel support."
        soap_response = create_soap_response(error_message)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "text/xml"},
            "body": soap_response
        }
