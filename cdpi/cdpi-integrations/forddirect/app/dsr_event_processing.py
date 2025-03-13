import boto3
import logging
import json
from os import environ
from typing import Any, Dict
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from datetime import datetime, timezone

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.audit_dsr import AuditDsr


# ✅ Logging Configuration
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

# ✅ AWS Clients
ssm = boto3.client("ssm")
sqs = boto3.client("sqs")


def get_sqs_url() -> str:
    """Retrieve the SQS URL from AWS SSM Parameter Store."""
    try:
        response = ssm.get_parameter(
            Name="/sqs/prod/SendEventToFordDirectUrl", WithDecryption=True
        )
        sqs_url = response["Parameter"]["Value"]
        logger.info(f"Retrieved SQS URL: {sqs_url}")
        return sqs_url
    except Exception as e:
        logger.error(f"Failed to retrieve SQS URL from SSM: {str(e)}")
        raise


def send_message_to_sqs(queue_url: str, message_body: Dict[str, Any]):
    """Send a message to the SQS queue."""
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body),
        )
        logger.info(f"Message sent to SQS: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Failed to send message to SQS: {str(e)}")
        raise


def record_handler(record: SQSRecord) -> Dict[str, Any]:
    """Process each record from the batch and send it to SQS."""
    logger.info(f"Processing Record: {record.body}")

    try:
        data = json.loads(record.body)
        consumer_id = data.get("consumer_id")
        event_type = data.get("event_type")
        completed_flag = data.get("completed_flag", False)

        source_consumer_id = data.get("source_consumer_id")
        integration_partner_id = data.get("integration_partner_id")

        # ✅ Database Transaction
        with DBSession() as session:
            audit_dsr = session.query(AuditDsr).filter(
                AuditDsr.consumer_id == consumer_id,
                AuditDsr.dsr_request_type == event_type,
            ).first()

            if audit_dsr:
                audit_dsr.complete_flag = completed_flag
                audit_dsr.complete_date = datetime.now(timezone.utc) if completed_flag else None
                audit_dsr.dsr_request_type = event_type

                session.commit()
                logger.info(f"AuditDsr updated for consumer_id {consumer_id}")

                # ✅ Send processed data to SQS queue
                sqs_url = get_sqs_url()
                send_message_to_sqs(sqs_url, data)

                return {"statusCode": 200, "body": {"message": "AuditDsr updated and sent to SQS"}}
            else:
                logger.warning(f"No existing AuditDsr found for consumer_id {consumer_id}")
                return {"statusCode": 404, "body": {"message": "No existing AuditDsr found"}}
    
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        return {"statusCode": 500, "body": {"message": "Internal server error"}}


def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
    logger.info(f"Received Event: {json.dumps(event)}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        return process_partial_response(
            event=event, record_handler=record_handler, processor=processor, context=context
        )
    except Exception as e:
        logger.exception("Error processing records")
        raise
