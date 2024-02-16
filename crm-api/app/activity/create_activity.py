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
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


class ValidationError(Exception):
    pass


def validate_activity_body(activity_type, due_ts, requested_ts, notes) -> None:
    """Validate activity body."""
    if activity_type == "note":
        if not notes:
            raise ValidationError("Notes are required for a note activity")
    elif activity_type == "appointment" or activity_type == "phone_call_task":
        if not due_ts:
            raise ValidationError("Activity due timestamp is required for an appointment or phone_call_task activity")


def create_on_crm(partner_name: str, payload: dict) -> None:
    """Create activity on CRM."""
    try:
        s3_key = f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
        queue_url = loads(
            s3_client.get_object(
                Bucket=INTEGRATIONS_BUCKET,
                Key=s3_key
            )["Body"].read().decode("utf-8")
        )["send_activity_queue_url"]

        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(payload)
        )
        logger.info(f"Sent activity {payload['activity_id']} to CRM")
    except Exception as e:
        logger.error(f"Error sending activity {payload['activity_id']} to CRM: {str(e)}")
        send_alert_notification(payload['activity_id'], e)


def send_alert_notification(activity_id: int, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred while sending activity {activity_id} to CRM: {e}",
    }
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps(data)}),
        Subject='CRM API: Activity Syndication Failure Alert',
        MessageStructure='json'
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        lead_id = event["queryStringParameters"]["lead_id"]

        # Timestamps are required as UTC with Zero Offset, eg. "2021-08-25T12:00:00Z"
        activity_type = body["activity_type"].lower()
        activity_due_ts = body.get("activity_due_ts")
        activity_requested_ts = body["activity_requested_ts"]
        notes = body.get("notes", "")
        contact_method = body.get("contact_method")

        validate_activity_body(activity_type, activity_due_ts, activity_requested_ts, notes)

        with DBSession() as session:
            # Check lead existence
            lead = session.query(Lead).filter(Lead.id == lead_id).first()
            if not lead:
                logger.error(f"Lead {lead_id} not found. Activity failed to be created.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead {lead_id} not found. Activity failed to be created."})
                }
            # OAS should validate activity type, this is a backup
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
                activity_due_ts=activity_due_ts,
                activity_requested_ts=activity_requested_ts,
                request_product=request_product,
                notes=notes,
                contact_method=contact_method
            )

            session.add(activity)

            session.commit()
            activity_id = activity.id
            logger.info(f"Created activity {activity_id}")

            dealer_partner = lead.consumer.dealer_integration_partner
            partner_name = dealer_partner.integration_partner.impel_integration_partner_name

            dealer_metadata = dealer_partner.dealer.metadata_
            if dealer_metadata:
                dealer_timezone = dealer_metadata.get("timezone", "")
            else:
                logger.warning(f"No metadata found for dealer: {dealer_partner.id}")
                dealer_timezone = ""

            payload = {
                # Lead info
                "lead_id": lead.id,
                "crm_lead_id": lead.crm_lead_id,
                "dealer_integration_partner_id": dealer_partner.id,
                "crm_dealer_id": dealer_partner.crm_dealer_id,
                "consumer_id": lead.consumer.id,
                "crm_consumer_id": lead.consumer.crm_consumer_id,
                # Activity info
                "activity_id": activity_id,
                "notes": activity.notes,
                "activity_due_ts": activity_due_ts,
                "activity_requested_ts": activity_requested_ts,
                "dealer_timezone": dealer_timezone,
                "activity_type": activity.activity_type.type,
                "contact_method": activity.contact_method,
            }

            logger.info(f"Payload to CRM: {dumps(payload)}")

            create_on_crm(partner_name=partner_name, payload=payload)

        return {
            "statusCode": "201",
            "body": dumps({"activity_id": activity_id})
        }

    except ValidationError as e:
        logger.error(f"Error creating activity: {str(e)}")
        return {
            "statusCode": "400",
            "body": dumps({"error": str(e)})
        }
    except Exception as e:
        logger.error(f"Error creating activity: {str(e)}")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
