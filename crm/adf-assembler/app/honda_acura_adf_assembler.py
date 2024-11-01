import logging
from os import environ
from typing import Any
from json import loads
from shared.oem_adf_creation import OemAdfCreation
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        logger.info(f"Event: {record}")
        body = loads(record["body"]) if record.get("body") else record

        logger.info(f"Processing record: {record}")

        oem_partner = body.get("oem_partner", {})

        oem_class = OemAdfCreation(oem_partner)
        oem_class.create_adf_data(body.get('lead_id'))
    except Exception as e:
        logger.exception(f"Error occurred while creating or sending ADF to JDPA.\n{e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process and syndicate event to ADF Assembler."""
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