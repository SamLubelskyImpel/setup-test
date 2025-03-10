import logging
from os import environ
from typing import Any, Dict
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from datetime import datetime, timezone

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.audit_dsr import AuditDsr


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

def record_handler(record: SQSRecord) -> Dict[str, Any]:
    """Record handler to process each record from the batch."""
    logger.info(f"Record: {record.body}")
    data = record.body

    consumer_id = data.get("consumer_id")
    dealer_id = data.get("dealer_id")
    event_type = data.get("event_type")
    completed_flag = data.get("completed_flag", False)

    with DBSession() as session:
        # Query the existing AuditDsr record
        audit_dsr = session.query(AuditDsr).filter(
            AuditDsr.consumer_id == consumer_id,
            AuditDsr.dsr_request_type == event_type
        ).first()

        if audit_dsr:
            # Update the record
            audit_dsr.complete_flag = completed_flag
            audit_dsr.complete_date = datetime.now(timezone.utc) if completed_flag else None
            audit_dsr.dsr_request_type = event_type
            
            session.commit()
            return {
                "statusCode": 200,
                "body": {"message": "AuditDsr updated successfully"}
            }
        else:
            # Log and return if no record is found
            return {
                "statusCode": 404,
                "body": {"message": f"No existing AuditDsr found for consumer_id {consumer_id}"}
            }

    # Add your business logic here
    return record



def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
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
        logger.exception(f"Error processing records")
        raise