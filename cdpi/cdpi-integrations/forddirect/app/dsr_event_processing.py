import logging
import json
from os import environ
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from datetime import datetime, timezone
from utils import send_alert_notification, send_message_to_sqs
from uuid import uuid4
from typing import Any

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.audit_dsr import AuditDsr


FORD_DIRECT_QUEUE_URL = environ.get("FORD_DIRECT_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord):
    """Process each record from the batch and send it to SQS."""
    logger.info(f"Processing Record: {record.body}")

    try:
        data = json.loads(record.body).get("detail", {})
        consumer_id = data.get("consumer_id")
        event_type = data.get("event_type")
        completed_flag = data.get("completed_flag", False)

        if not consumer_id or not event_type:
            logger.warning(f"[Validation] Missing required fields in record | Data: {data}")
            raise ValueError("Missing required fields: consumer_id or event_type")

        with DBSession() as session:
            audit_dsr = session.query(
                AuditDsr
            ).filter(
                AuditDsr.consumer_id == consumer_id,
                AuditDsr.dsr_request_type == event_type,
            ).order_by(
                AuditDsr.request_date.desc()
            ).first()

            if not audit_dsr:
                logger.warning(f"[DB] No matching AuditDsr found | ConsumerID: {consumer_id} | EventType: {event_type}")
                raise Exception(f"No existing AuditDsr found for consumer_id {consumer_id}")

            audit_dsr.complete_flag = completed_flag
            audit_dsr.complete_date = datetime.now(timezone.utc) if completed_flag else None
            dsr_request_id = audit_dsr.dsr_request_id

            session.commit()
            logger.info(f"[DB] Updated AuditDsr | ConsumerID: {consumer_id} | CompletedFlag: {completed_flag}")
            data["dsr_request_id"] = dsr_request_id

            if completed_flag:
                logger.info(f"Sending message to Ford Direct Queue URL: {FORD_DIRECT_QUEUE_URL}")
                send_message_to_sqs(str(FORD_DIRECT_QUEUE_URL), data)
            else:
                logger.warning("DSR request was not completed and will not be forwarded to FD.")

    except Exception as e:
        logger.exception(f"[Handler] Error processing record | ConsumerID: {data.get('consumer_id', 'Unknown')} | Error: {e}")
        raise Exception("Internal server error")


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
        send_alert_notification(request_id, 'dsr_event_processing', e)

        logger.exception("Error processing records")
        raise
