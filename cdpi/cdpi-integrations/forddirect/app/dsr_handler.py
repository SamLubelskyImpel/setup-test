import logging
import json
from os import environ
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from datetime import datetime, timezone
from utils import send_alert_notification, send_message_to_sqs, create_audit_dsr, call_events_api
from uuid import uuid4
from typing import Any

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer_profile import ConsumerProfile
from cdpi_orm.models.integration_partner import IntegrationPartner


FORD_DIRECT_QUEUE_URL = environ.get("FORD_DIRECT_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def handle_delete(
    session, consumer_profile_db, consumer_id,
    event_type, dsr_request_id, dsr_request_timestamp,
    source_consumer_id, source_dealer_id, dealer_id,
    product_name, integration_partner_id, is_enterprise
):
    logger.info("Processing delete request")
    # Don't delete old data
    dt = datetime.fromisoformat(dsr_request_timestamp.replace("Z", "+00:00"))
    if ((consumer_profile_db.score_update_date and consumer_profile_db.score_update_date > dt)
       or consumer_profile_db.db_creation_date > dt):
        logger.info("DSR Request Timestamp prior to latest score. Auto-completing delete request.")
        audit_dsr = create_audit_dsr(
            integration_partner_id,
            consumer_id,
            event_type,
            dsr_request_id,
            dsr_request_timestamp
        )
        session.add(audit_dsr)
        send_message_to_sqs(str(FORD_DIRECT_QUEUE_URL), {
            "consumer_id": consumer_id,
            "dealer_id": dealer_id,
            "event_type": event_type,
            "dsr_request_id": dsr_request_id,
            "is_enterprise": is_enterprise,
            "completed_flag": False
        })
        session.commit()
        return

    logger.info("Standard Deletion Request")
    session.delete(consumer_profile_db)

    audit_dsr = create_audit_dsr(
        integration_partner_id,
        consumer_id,
        event_type,
        dsr_request_id,
        dsr_request_timestamp
    )
    session.add(audit_dsr)
    call_events_api(event_type, consumer_id, source_consumer_id, dealer_id, source_dealer_id, product_name)
    session.commit()


def handle_optout(
    session, consumer_profile_db, consumer_id,
    event_type, dsr_request_id, dsr_request_timestamp,
    source_consumer_id, source_dealer_id, dealer_id,
    product_name, integration_partner_id
):
    logger.info("Processing optout request")
    session.delete(consumer_profile_db)
    audit_dsr = create_audit_dsr(
        integration_partner_id,
        consumer_id,
        event_type,
        dsr_request_id,
        dsr_request_timestamp
    )
    session.add(audit_dsr)
    call_events_api(event_type, consumer_id, source_consumer_id, dealer_id, source_dealer_id, product_name)
    session.commit()


def record_handler(record: SQSRecord):
    logger.info(f"Processing Record: {record.body}")

    try:
        body = json.loads(record['body'])

        consumer_id = body["consumer_id"]
        dealer_id = body["dealer_id"]
        source_consumer_id = body["source_consumer_id"]
        source_dealer_id = body["source_dealer_id"]
        product_name = body["product_name"]
        event_type = body["event_type"]
        dsr_request_id = body["dsr_request_id"]
        dsr_request_timestamp = body["dsr_request_timestamp"]
        is_enterprise = body["is_enterprise"]

        # lookup consumer profile
        with DBSession() as session:
            consumer_profile_db = session.query(
                    ConsumerProfile
                ).filter(
                    ConsumerProfile.consumer_id == consumer_id
                ).first()

            if not consumer_profile_db:
                integration_partner_db = session.query(
                        IntegrationPartner
                    ).filter(
                        IntegrationPartner.impel_integration_partner_name == "FORD_DIRECT"
                    ).first()

                # Consumer profile doesn't exist
                logger.warning(f"Consumer Profile not found for consumer_id {consumer_id}. Completing request.")
                audit_dsr = create_audit_dsr(
                    integration_partner_db.id,
                    consumer_id,
                    event_type,
                    dsr_request_id,
                    dsr_request_timestamp,
                    complete_date=datetime.now(timezone.utc),
                    complete_flag=True
                )
                session.add(audit_dsr)
                send_message_to_sqs(str(FORD_DIRECT_QUEUE_URL), {
                    "consumer_id": consumer_id,
                    "dealer_id": dealer_id,
                    "event_type": event_type,
                    "dsr_request_id": dsr_request_id,
                    "is_enterprise": is_enterprise,
                    "completed_flag": True
                })
                session.commit()
                return

            integration_partner_id = consumer_profile_db.integration_partner_id
            common_args = {
                'session': session,
                'consumer_profile_db': consumer_profile_db,
                'consumer_id': consumer_id,
                'event_type': event_type,
                'dsr_request_id': dsr_request_id,
                'dsr_request_timestamp': dsr_request_timestamp,
                'source_consumer_id': source_consumer_id,
                'source_dealer_id': source_dealer_id,
                'dealer_id': dealer_id,
                'product_name': product_name,
                'integration_partner_id': integration_partner_id
            }

            if event_type == "cdp.dsr.delete":
                handle_delete(**common_args, is_enterprise=is_enterprise)
            elif event_type == "cdp.dsr.optout":
                handle_optout(**common_args)

    except Exception as e:
        logger.error(e)
        raise


def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
    logger.info(f"Received Event: {json.dumps(event)}")
    request_id = str(uuid4())

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        return process_partial_response(
            event=event, record_handler=record_handler,
            processor=processor, context=context
        )
    except Exception as e:
        send_alert_notification(request_id, 'dsr_handler', e)

        logger.exception("Error processing records")
        raise
