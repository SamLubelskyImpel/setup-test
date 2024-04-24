import json
from json import loads
import logging
import os
import boto3
from os import environ
from typing import Any
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
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")

    #     crm_lead_id = json_data["id"]
    #     crm_dealer_id = json_data["dealerID"]
    #     lead_status = json_data.get("leadStatus", "")
    #     lead_status_timestamp = json_data.get("leadStatusTimestamp", "")

    #     crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]
    #     lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)

    #     if not lead_id:
    #         logger.error(f"Could not retrieve lead ID for CRM lead ID: {crm_lead_id}")
    #         raise ValueError("Lead ID could not be retrieved.")

    #     data = {'metadata': {}}

    #     if lead_status:
    #         data['lead_status'] = lead_status
    #     else:
    #         logger.warning(f"Lead status is empty for CRM lead ID: {crm_lead_id}. No update will be performed for this field.")
    #     if lead_status_timestamp:
    #         data['metadata']['leadStatusTimestamp'] = lead_status_timestamp

    #     if json_data.get("bdcID"):
    #         salesperson = update_lead_salespersons(json_data["bdcID"], json_data.get("bdcName", ""), lead_id, crm_api_key, "BDC Rep")
    #         data['salespersons'] = salesperson
    #     elif json_data.get("contactID"):
    #         logger.info("No BDC rep found. Using sales rep as salesperson.")
    #         salesperson = update_lead_salespersons(json_data["contactID"], json_data.get("contactName", ""), lead_id, crm_api_key)
    #         data['salespersons'] = salesperson
    #     else:
    #         logger.warning(f"Salesperson info (BDC/sales rep) not included. CRM lead ID: {crm_lead_id}. No update will be performed for salespersons.")

    #     if 'lead_status' in data or 'salespersons' in data:
    #         update_lead_status(lead_id, data, crm_api_key)
    #         logger.info(f"Lead {crm_lead_id} updated successfully.")
    #     else:
    #         logger.info(f"No updates to apply for lead {crm_lead_id}.")

    except Exception as e:
        logger.error(f"Error transforming momentum lead update record - {record}: {e}")
        # logger.error("[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] ProductDealerId: {}\nLeadId: {}\nCrmDealerId: {}\nCrmLeadId: {}\nTraceback: {}".format(
        #     product_dealer_id, lead_id, crm_dealer_id, crm_lead_id, e)
        # )
        raise


def lambda_handler(event, context):
    """Transform raw activix lead update data to the unified format."""
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
