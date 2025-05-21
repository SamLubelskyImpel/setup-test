"""Create lead in the shared CRM layer."""

import pytz
import boto3
import logging
from os import environ
from dateutil import parser
from json import dumps, loads
from typing import Any, List
from sqlalchemy.exc import SQLAlchemyError
from lead.utils import send_alert_notification

from common.validation import validate_request_body, ValidationErrorResponse
from common.models.create_lead import CreateLeadRequest, VehicleOfInterest
from common.utils import send_message_to_event_enricher
from event_service.events import dispatch_event, Event, Resource

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.session_config import DBSession
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner


ENVIRONMENT = environ.get("ENVIRONMENT")
EVENT_LISTENER_QUEUE = environ.get("EVENT_LISTENER_QUEUE")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")

salesperson_attrs = [
    "dealer_integration_partner_id",
    "crm_salesperson_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "position_name",
    "is_primary",
]


class DASyndicationError(Exception):
    """Custom exception for DA syndication errors."""
    pass


def send_notification_to_event_listener(integration_partner_name: str) -> bool:
    """Check if notification should be sent to the event listener."""
    try:
        response = s3_client.get_object(
            Bucket=INTEGRATIONS_BUCKET,
            Key=f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_GENERAL.json",
        )
        config = loads(response["Body"].read())
        logger.info(f"Config: {config}")
        notification_partners = config["notification_partners"]
        if integration_partner_name in notification_partners:
            return True
    except Exception as e:
        raise DASyndicationError(e)
    return False


def update_attrs(
    db_object: Any,
    data: Any,
    dealer_partner_id: str,
    allowed_attrs: List[str],
    request_product,
) -> None:
    """Update attributes of a database object."""
    combined_data = {"dealer_integration_partner_id": dealer_partner_id, **data}

    for attr in allowed_attrs:
        if attr in combined_data:
            setattr(db_object, attr, combined_data[attr])


def process_lead_ts(input_ts: Any, dealer_timezone: Any) -> Any:
    """
    Process an input timestamp based on whether it's in UTC, has an offset, or is local without an offset.

    Apply dealer_timezone if it is present.
    """
    try:
        parsed_ts = parser.parse(input_ts)

        # Check if the timestamp is already in UTC (ends with 'Z')
        if (
            parsed_ts.tzinfo is not None
            and parsed_ts.tzinfo.utcoffset(parsed_ts) is not None
        ):
            # Timestamp is either UTC or has an offset; return in ISO format
            return parsed_ts.isoformat()

        # If the timestamp is local (no 'Z' or offset) and dealer_timezone is provided
        if dealer_timezone:
            dealer_tz = pytz.timezone(dealer_timezone)
            # Localize the timestamp to the dealer's timezone
            localized_ts = dealer_tz.localize(parsed_ts)
            return localized_ts.isoformat()
        else:
            # Timestamp is local without a specified dealer_timezone
            # Returning the naive timestamp as it is
            return parsed_ts.isoformat()

    except Exception as e:
        logger.info(
            f"Error processing timestamp: {input_ts}, Dealer timezone: {dealer_timezone}. Error: {e}"
        )
        return None


def lambda_handler(event: Any, context: Any) -> Any:
    """Create lead."""
    try:
        logger.info(f"Event: {event}")
        body: CreateLeadRequest = validate_request_body(event, CreateLeadRequest)
        request_product = event["headers"]["partner_id"]
        consumer_id = body.consumer_id
        salespersons = body.salespersons
        crm_lead_id = body.crm_lead_id
        lead_ts = body.lead_ts
        logger.info(f"Body: {body.model_dump()}")

        authorizer_integration_partner = event["requestContext"]["authorizer"]["integration_partner"]

        with DBSession() as session:
            try:
                db_results = session.query(
                    Consumer, DealerIntegrationPartner, Dealer, IntegrationPartner
                ).join(
                    DealerIntegrationPartner, DealerIntegrationPartner.id == Consumer.dealer_integration_partner_id
                ).join(
                    Dealer, Dealer.id == DealerIntegrationPartner.dealer_id
                ).join(
                    IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
                ).filter(
                    Consumer.id == consumer_id
                ).first()

                if not db_results:
                    logger.error(f"Consumer {consumer_id} not found")
                    return {
                        "statusCode": 404,
                        "body": dumps(
                            {
                                "error": f"Consumer {consumer_id} not found. Lead failed to be created."
                            }
                        ),
                    }

                consumer_db, dip_db, dealer_db, integration_partner_db = db_results
                product_dealer_id = dealer_db.product_dealer_id
                dip_metadata = dip_db.metadata_
                integration_partner_name = integration_partner_db.impel_integration_partner_name

                if authorizer_integration_partner:
                    if integration_partner_name != authorizer_integration_partner:
                        return {
                            "statusCode": 401,
                            "body": dumps(
                                {
                                    "error": "This request is unauthorized. The authorization credentials are missing or are wrong. For example, the partner_id or the x_api_key provided in the header are wrong/missing."
                                }
                            ),
                        }

                if not any([dip_db.is_active, dip_db.is_active_salesai, dip_db.is_active_chatai]):
                    error_msg = f"Dealer integration partner {dip_db.id} is not active. Lead failed to be created."
                    logger.error(error_msg)
                    send_alert_notification(subject='CRM API: Lead creation failure', message=error_msg)
                    return {
                        "statusCode": 404,
                        "body": dumps({"error": error_msg})
                    }

                dealer_metadata = dealer_db.metadata_
                if dealer_metadata:
                    dealer_timezone = dealer_metadata.get("timezone", "UTC")
                else:
                    logger.warning(f"No metadata found for dealer: {product_dealer_id}. Defaulting to UTC.")
                    dealer_timezone = "UTC"
                logger.info(f"Dealer timezone: {dealer_timezone}")

                logger.info(f"Original timestamp: {lead_ts}")
                lead_ts = process_lead_ts(lead_ts, dealer_timezone)
                logger.info(f"Processed timestamp: {lead_ts}")

                # Query for existing lead
                if crm_lead_id:
                    lead_db = (
                        session.query(Lead)
                        .filter(
                            Lead.consumer_id == consumer_id,
                            Lead.crm_lead_id == crm_lead_id
                        )
                        .first()
                    )

                    if lead_db:
                        logger.error(f"Lead {crm_lead_id} already exists")
                        return {
                            "statusCode": 409,
                            "body": dumps(
                                {
                                    "error": f"Lead with CRM ID {crm_lead_id} already exists for consumer {consumer_id}. lead_id: {lead_db.id}"
                                }
                            ),
                        }

                # Create lead
                lead = Lead(
                    consumer_id=consumer_id,
                    status=body.lead_status,
                    substatus=body.lead_substatus,
                    comment=body.lead_comment,
                    origin_channel=body.lead_origin,
                    source_channel=body.lead_source,
                    source_detail=body.lead_source_detail,
                    crm_lead_id=crm_lead_id,
                    request_product=request_product,
                    lead_ts=lead_ts,
                    metadata_=body.metadata.model_dump() if body.metadata else None,
                )

                session.add(lead)
                logger.info("Lead pending")

                # Create vehicles of interest
                vehicles_of_interest: List[VehicleOfInterest] = body.vehicles_of_interest or []
                for vehicle in vehicles_of_interest:
                    vo_data = vehicle.model_dump()
                    vo_data["stock_num"] = vo_data.pop("stock_number", None)
                    vo_data["vehicle_class"] = vo_data.pop("class_", None)
                    vo_data["manufactured_year"] = vo_data.pop("year", None)

                    vo_data["metadata_"] = vehicle.metadata.model_dump() if vehicle.metadata else None

                    vehicle = Vehicle(**vo_data)
                    lead.vehicles.append(vehicle)

                if salespersons:
                    for salesperson in salespersons:
                        # Create salesperson
                        crm_salesperson_id = salesperson.crm_salesperson_id

                        # Query for existing salesperson
                        salesperson_db = None
                        if crm_salesperson_id:
                            salesperson_db = (
                                session.query(Salesperson)
                                .filter(
                                    Salesperson.crm_salesperson_id == crm_salesperson_id,
                                    Salesperson.dealer_integration_partner_id == dip_db.id,
                                )
                                .first()
                            )

                        if not salesperson_db:
                            salesperson_db = Salesperson()

                        update_attrs(
                            salesperson_db,
                            salesperson.model_dump(),
                            dip_db.id,
                            salesperson_attrs,
                            request_product,
                        )

                        if not salesperson_db.id:
                            logger.info("Salesperson pending")

                        # Create lead salesperson
                        lead_salesperson = Lead_Salesperson(
                            is_primary=salesperson.is_primary,
                        )
                        lead_salesperson.salesperson = salesperson_db
                        lead_salesperson.lead = lead
                        session.add(lead_salesperson)

                session.commit()
                logger.info("Transactions committed")
                lead_id = lead.id

                payload_details = {
                    "lead_id": lead_id,
                    "consumer_id": consumer_id,
                    "source_application": event["requestContext"]["authorizer"]["source_application"],
                    "idp_dealer_id": dealer_db.idp_dealer_id,
                    "event_type": "Lead Created",
                }

                send_message_to_event_enricher(payload_details)

            except SQLAlchemyError as e:
                # Rollback in case of any error
                session.rollback()
                logger.info(f"Error occurred: {e}")
                raise e

        logger.info(f"Created lead {lead_id}")
        logger.info(f"Integration partner: {integration_partner_name}")

        dispatch_event(
            request_product=request_product,
            partner=integration_partner_name,
            event=Event.Created,
            resource=Resource.Lead,
            content={
                'message': 'Lead Created',
                'lead_id': lead_id,
                'consumer_id': consumer_id,
                'dealer_id': product_dealer_id,
            })

        notify_listener = send_notification_to_event_listener(integration_partner_name)

        if request_product == "chat_ai":
            logger.info("Skipping notification to Event Listener for chat_ai. Handled by Event Enricher.")
        elif notify_listener:
            try:
                sqs_client = boto3.client('sqs')

                sqs_client.send_message(
                    QueueUrl=EVENT_LISTENER_QUEUE,
                    MessageBody=dumps({"lead_id": lead_id})
                )
            except Exception as e:
                raise DASyndicationError(e)

    except ValidationErrorResponse as e:
        logger.warning(f"Validation failed: {e.full_errors}")
        return {
            "statusCode": 400,
            "body": dumps({
                "message": "Validation failed",
                "errors": e.errors,
            }),
        }

    except DASyndicationError as e:
        logger.error(f"Error syndicating lead: {e}.")
        send_alert_notification(
            message=f"Error occurred while sending lead {lead_id} to EventListener: {e}",
            subject="Lead Syndication Failure Alert - CreateLead"
        )

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        send_alert_notification(
            message=f"Error occurred while creating lead: {e}",
            subject="Lead Creation Failure Alert - CreateLead"
        )
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }

    return {"statusCode": 201, "body": dumps({"lead_id": lead_id})}
