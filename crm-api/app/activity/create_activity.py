"""Create activity."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
import boto3

from crm_orm.models.lead import Lead
from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def create_on_crm(partner_name: str, activity: Activity, crm_dealer_id: str):
    s3_key = f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
    fifo_queue = loads(
        s3_client.get_object(
            Bucket=INTEGRATIONS_BUCKET, 
            Key=s3_key
        )["Body"].read().decode("utf-8")
    )["transform_unified_data_queue_url"]
    sqs_client.send_message(
        QueueUrl=fifo_queue,
        MessageBody=dumps({
            "activity_id": activity.id,
            "lead_id": activity.lead_id,
            "notes": activity.notes,
            "activity_due_ts": activity.activity_due_ts,
            "crm_dealer_id": crm_dealer_id
        }),
        MessageGroupId=partner_name
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        lead_id = event["pathParameters"]["lead_id"]

        activity_type = body["activity_type"].lower()
        activity_due_ts = body.get("activity_due_ts")
        activity_requested_ts = body["activity_requested_ts"]
        notes = body.get("notes", "")

        with DBSession() as session:
            # Check lead existence
            lead = session.query(Lead).filter(Lead.id == lead_id).first()
            if not lead:
                logger.error(f"Lead {lead_id} not found. Activity failed to be created.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead {lead_id} not found. Activity failed to be created."})
                }

            activity_type_db = session.query(ActivityType).filter(ActivityType.type == activity_type).first()
            if not activity_type_db:
                logger.error(f"Failed to find activity type {activity_type} for lead {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Activity type {activity_type} not found."})
                }

            # Create activity
            activity = Activity(
                lead_id=lead.id,
                activity_type_id=activity_type_db.id,
                activity_requested_ts=activity_requested_ts,
                request_product=request_product,
                notes=notes
            )
            if activity_due_ts:
                activity.activity_due_ts = activity_due_ts

            session.add(activity)
            
            create_on_crm(
                partner_name=lead.consumer.dealer.integration_partner.impel_integration_partner_name, 
                activity=activity,
                crm_dealer_id=lead.consumer.dealer.crm_dealer_id)
            
            session.commit()
            activity_id = activity.id

        logger.info(f"Created activity {activity_id}")

        return {
            "statusCode": "201",
            "body": dumps({"activity_id": activity_id})
        }

    except Exception as e:
        logger.exception(f"Error creating activity")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
