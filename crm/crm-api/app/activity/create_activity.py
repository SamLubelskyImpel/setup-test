"""Create activity."""
import logging
from os import environ
from requests import post
from json import dumps, loads
from typing import Any
import boto3
import botocore.exceptions
from .utils import apply_dealer_timezone, send_general_alert_notification
from common.utils import send_message_to_event_enricher

from crm_orm.models.lead import Lead
from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import DBSession
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer

from sqlalchemy.dialects.postgresql import insert

from event_service.events import dispatch_event, Event, Resource

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
ADF_ASSEMBLER_QUEUE = environ.get("ADF_ASSEMBLER_QUEUE")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
secret_client = boto3.client("secretsmanager")


class ADFAssemblerSyndicationError(Exception):
    """Custom exception for ADF Assembler syndication errors."""
    pass


class ValidationError(Exception):
    pass


def make_adf_assembler_request(data: Any):
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/adf-assembler"
    )
    secret = loads(secret["SecretString"])["create_adf"]
    api_url, api_key = loads(secret).values()

    response = post(
        url=api_url,
        data=dumps(data),
        headers={
            "x_api_key": api_key,
            "action_id": "create_adf",
            'Content-Type': 'application/json'
        }
    )

    logger.info(f"StatusCode: {response.status_code}; Text: {response.json()}")


def validate_activity_body(activity_type, due_ts, requested_ts, notes) -> None:
    """Validate activity body."""
    if activity_type == "note":
        if not notes:
            raise ValidationError("Notes are required for a note activity")
    elif activity_type == "appointment" or activity_type == "phone_call_task":
        if not due_ts:
            raise ValidationError("Activity due timestamp is required for an appointment or phone_call_task activity")


def is_writeback_disabled(partner_name: str, activity_id: int) -> bool:
    """Check if writeback is disabled for the partner."""
    try:
        s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_GENERAL.json"
        config = loads(
            s3_client.get_object(
                Bucket=INTEGRATIONS_BUCKET,
                Key=s3_key
            )["Body"].read().decode("utf-8")
        )
        logger.info(f"Config: {config}")
        disabled_partners = config["writeback_disabled_partners"]
        if partner_name in disabled_partners:
            return True

    except Exception as e:
        logger.error(f"Error checking writeback status for {partner_name}: {str(e)}")
        send_alert_notification(activity_id, e)

    return False


def create_on_crm(partner_name: str, payload: dict) -> None:
    """Create activity on CRM."""
    try:
        s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_{partner_name.upper()}.json"
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

    except botocore.exceptions.ClientError as e:
        logger.error(f"Error retrieving configuration file for {partner_name}")
        send_alert_notification(payload['activity_id'], e)

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
            # OAS should validate activity type, this is a backup
            activity_type_id, = session.query(ActivityType.id).filter(ActivityType.type == activity_type).first()
            if not activity_type_id:
                logger.error(f"Failed to find activity type {activity_type} for lead {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Activity type {activity_type} not found."})
                }

            # Check lead existence
            db_results = session.query(
                Lead.id,
                Lead.crm_lead_id,
                Consumer.id,
                Consumer.crm_consumer_id,
                DealerIntegrationPartner.id,
                DealerIntegrationPartner.crm_dealer_id,
                DealerIntegrationPartner.metadata_,
                DealerIntegrationPartner.is_active,
                DealerIntegrationPartner.is_active_salesai,
                DealerIntegrationPartner.is_active_chatai,
                Dealer.metadata_,
                Dealer.product_dealer_id,
                Dealer.idp_dealer_id,
                IntegrationPartner.impel_integration_partner_name
            ).join(
                Consumer, Lead.consumer_id == Consumer.id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Lead.id == lead_id
            ).first()

            if not db_results:
                logger.error(f"Lead {lead_id} not found. Activity failed to be created.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead {lead_id} not found. Activity failed to be created."})
                }

            (lead_id,
             crm_lead_id,
             consumer_id,
             crm_consumer_id,
             dip_id,
             crm_dealer_id,
             dip_metadata,
             dip_is_active,
             dip_is_active_salesai,
             dip_is_active_chatai,
             dealer_metadata,
             product_dealer_id,
             idp_dealer_id,
             partner_name) = db_results

            if not any([dip_is_active, dip_is_active_salesai, dip_is_active_chatai]):
                error_msg = f"Dealer integration partner {dip_id} is not active. Activity failed to be created."
                logger.error(error_msg)
                send_general_alert_notification(subject='CRM API: Activity creation failure', message=error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }

            # Create activity
            stmt = insert(Activity).values(
                lead_id=lead_id,
                activity_type_id=activity_type_id,
                activity_due_ts=activity_due_ts,
                activity_requested_ts=activity_requested_ts,
                request_product=request_product,
                notes=notes,
                contact_method=contact_method
            ).returning(Activity.id)

            activity_id = session.execute(stmt).scalar()
            session.commit()
            logger.info(f"Created activity {activity_id}")

        dispatch_event(
            request_product=request_product,
            partner=partner_name,
            event=Event.Created,
            resource=Resource.Activity,
            content={
                'message': 'Activity Created',
                'activity_id': activity_id,
                'lead_id': lead_id,
                'dealer_id': product_dealer_id,
                'activity_type': activity_type
            })

        if dealer_metadata:
            dealer_timezone = dealer_metadata.get("timezone", "")
        else:
            logger.warning(f"No metadata found for dealer: {dip_id}")
            dealer_timezone = ""

        payload = {
            # Lead info
            "lead_id": lead_id,
            "crm_lead_id": crm_lead_id,
            "dealer_integration_partner_id": dip_id,
            "crm_dealer_id": crm_dealer_id,
            "consumer_id": consumer_id,
            "crm_consumer_id": crm_consumer_id,
            "dealer_integration_partner_metadata": dip_metadata,
            # Activity info
            "activity_id": activity_id,
            "notes": notes,
            "activity_due_ts": activity_due_ts,
            "activity_requested_ts": activity_requested_ts,
            "dealer_timezone": dealer_timezone,
            "activity_type": activity_type,
            "contact_method": contact_method,
        }

        logger.info(f"Payload to CRM: {dumps(payload)}")

        writeback_disabled = is_writeback_disabled(partner_name, activity_id)

        # If activity is going to be sent to the CRM as an ADF, don't send it to the CRM as a normal activity
        if request_product == "chat_ai" and activity_type == "appointment":
            try:
                if dip_metadata:
                    oem_partner = dip_metadata.get("oem_partner", {})
                else:
                    logger.warning(f"No metadata found for dealer: {dip_id}")
                    oem_partner = {}

                # As the salesrep will be reading the ADF file, we need to convert the activity_due_ts to the dealer's timezone.
                activity_due_dealer_ts = apply_dealer_timezone(
                    activity_due_ts, dealer_timezone, dip_id
                )
                payload = {
                    "event_type": "Appointment",
                    "lead_id": lead_id,
                    "partner_name": partner_name,
                    "oem_partner": oem_partner,
                    "activity_time": activity_due_dealer_ts,
                    "product_dealer_id": product_dealer_id,
                }

                sqs_client = boto3.client('sqs')

                sqs_client.send_message(
                    QueueUrl=ADF_ASSEMBLER_QUEUE,
                    MessageBody=dumps(payload)
                )
            except Exception as e:
                raise ADFAssemblerSyndicationError(e)

        elif writeback_disabled:
            logger.info(f"Partner {partner_name} disabled custom writeback. Activity {activity_id} will not be sent to integration resource.")
        else:
            create_on_crm(partner_name=partner_name, payload=payload)

        payload_details = {
            "lead_id": lead_id,
            "activity_id": activity_id,
            "source_application": event["requestContext"]["authorizer"]["source_application"],
            "idp_dealer_id": idp_dealer_id,
            "event_type": "Activity Created",
        }

        send_message_to_event_enricher(payload_details)

        return {
            "statusCode": 201,
            "body": dumps({"activity_id": activity_id})
        }

    except ADFAssemblerSyndicationError as e:
        logger.error(f"Error syndicating activity {activity_id} to ADF Assembler: {e}.")
        send_alert_notification(activity_id, e)
    except ValidationError as e:
        logger.error(f"Error creating activity: {str(e)}")
        return {
            "statusCode": 400,
            "body": dumps({"error": str(e)})
        }
    except Exception as e:
        logger.error(f"Error creating activity: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
