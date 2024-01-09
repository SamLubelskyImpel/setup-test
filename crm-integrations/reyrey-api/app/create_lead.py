import logging
import boto3
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from shared_resources.app.get_active_dealers import get_dealers
from os import environ
from typing import Any
from uuid import uuid4

BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def lambda_handler(event: Any, context: Any) -> Any:
    """This API handler takes the XML sent by ReyRey and puts the raw XML into the S3 bucket."""
    try:
        logger.info(f"Event: {event}")

        lead_xml_body = event["body"]
        reyrey_dealer_list = get_dealers("reyrey_crm")
        crm_dealer_id = extract_crm_dealer_id(lead_xml_body)
        product_dealer_id = None

        for dealer in reyrey_dealer_list:
            if dealer["crm_dealer_id"] == crm_dealer_id:
                product_dealer_id = dealer["product_dealer_id"]
                break

        if not product_dealer_id:
            logger.error(f"Dealer {crm_dealer_id} not found in active dealers.")
            return {
                "statusCode": 401,
                "body": {
                    "error": "This request is unauthorized. The authorization credentials are missing or are wrong. For example if the partner_id or the x_api_key provided in the header are wrong/missing. This error can also occur if the dealer_id provided hasn't been configured with Impel."
                },
            }

        logger.info("New lead received for dealer: " + product_dealer_id)

        save_raw_lead(lead_xml_body, product_dealer_id)

        return {"statusCode": 200, "body": "Success."}
    except ValueError as e:
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        logger.error(f"Error creating consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "Internal Server Error. Please contact Impel support."}
            ),
        }


def save_raw_lead(lead: str, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/reyrey_crm/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving reyrey lead to {s3_key}")
    s3_client.put_object(
        Body=lead,
        Bucket=BUCKET,
        Key=s3_key,
    )


def extract_crm_dealer_id(lead_xml_body: str) -> str:
    """Extract CRM dealer ID from incoming xml."""
    root = ET.fromstring(lead_xml_body)
    if (
        root.tag
        != "{http://www.starstandards.org/STAR}rey_ImpelCRMPublishLeadDisposition"
    ):
        raise ValueError("Invalid XML format")

    namespace = {"star": "http://www.starstandards.org/STAR"}

    dealer_number = root.find(".//star:DealerNumber", namespace).text
    store_number = root.find(".//star:StoreNumber", namespace).text
    area_number = root.find(".//star:AreaNumber", namespace).text

    concatenated_dealer_id = f"{dealer_number}_{store_number}_{area_number}"

    return concatenated_dealer_id
