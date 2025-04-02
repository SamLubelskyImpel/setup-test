import logging
from os import environ
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from rds_instance import RDSInstance
import boto3
from json import dumps
from tekion_wrapper import TekionWrapper
from requests.exceptions import HTTPError
from datetime import datetime, timezone


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")


def save_response(res, dms_id, dms_customer_id):
    now = datetime.now(tz=timezone.utc)
    key = f"tekion-apc/customer/{now.year}/{now.month}/{now.day}/{dms_id}_{dms_customer_id}.json"
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=key,
        Body=dumps(res)
    )
    logger.info(f"Saved raw response to {key}")


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        body = record.json_body
        dms_id = body.get("dms_id")
        customer_id = body.get("customer_id")
        dms_customer_id = body.get("dms_customer_id")

        api = TekionWrapper(dealer_id=dms_id)
        db_instance = RDSInstance()

        tekion_res = api.get_customer(dms_customer_id)
        save_response(tekion_res, dms_id, dms_customer_id)

        if len(tekion_res) == 0:
            logger.error(f"Customer {dms_customer_id} not found for {dms_id}")
        else:
            communication_preferences = tekion_res[0].get("communicationPreferences", {})
            email_optin = communication_preferences.get("email", {}).get("serviceUpdates", False)
            phone_optin = communication_preferences.get("call", {}).get("serviceUpdates", False)
            mail_optin = communication_preferences.get("postalMail", {}).get("serviceUpdates", False)
            sms_optin = communication_preferences.get("text", {}).get("serviceUpdates", False)
            db_instance.update_optin(customer_id, email_optin, phone_optin, mail_optin, sms_optin)

        db_instance.set_optin_updated_flag(customer_id)
    except HTTPError as e:
        if e.response.status_code == 429:
            logger.error("Tekion API returned 429")
            return
        logger.exception("HTTP error")
        raise
    except Exception as e:
        logger.exception("Error processing record")
        raise


def enrich_customers_handler(event, context):
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
    except:
        logger.exception(f"Error processing batch")
        raise
