import logging
import os
from typing import Any
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.audit_log import AuditLog
from datetime import datetime, timedelta, timezone
from json import dumps
import boto3
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

FD_FILE_HEADER = 'ext_consumer_id|^|DEALER_IDENTIFIER|^|PII|^|PII_Type|^|Record_Date\n'
SHARED_BUCKET = os.environ.get('SHARED_BUCKET')
IS_PROD = int(os.environ.get('IS_PROD'))
PII_ADDRESS_MAP = {
    'address_line_1': 'addressline1',
    'address_line_2': 'addressline2',
    'suite': 'suite',
    'city': 'city',
    'areatype': 'areatype',
    'area': 'area',
    'country': 'country',
    'zip': 'zip',
    'zipextra': 'zipextra',
    'pobox': 'pobox',
}

s3_client = boto3.client('s3')


def make_address_json(consumer: dict):
    """Maps consumer address fields to the FD expected schema."""
    address_dict = { v: consumer.get(k) for k, v in PII_ADDRESS_MAP.items() if consumer.get(k) }
    return dumps(address_dict) if address_dict else None


def make_pii_rows(consumer_dict: dict, dealer_id: int) -> list:
    """Make PII rows for a consumer record."""
    pii_rows = []

    record_date = consumer_dict["record_date"]
    if not record_date:
        record_date = datetime.now(timezone.utc)
    elif isinstance(record_date, str):  # record_date becomes a str on the audit_log e.g. 2024-10-21 13:50:49+00
        record_date = datetime.strptime(record_date + '00', '%Y-%m-%d %H:%M:%S%z')
    record_date = record_date.strftime('%Y-%m-%dT%H:%M:%S')

    if 'first_name' in consumer_dict:
        pii_rows.append(f'{consumer_dict["id"]}|^|{dealer_id}|^|{consumer_dict["first_name"]}|^|fName|^|{record_date}\n')
    if 'last_name' in consumer_dict:
        pii_rows.append(f'{consumer_dict["id"]}|^|{dealer_id}|^|{consumer_dict["last_name"]}|^|lName|^|{record_date}\n')
    if 'email' in consumer_dict:
        pii_rows.append(f'{consumer_dict["id"]}|^|{dealer_id}|^|{consumer_dict["email"]}|^|email|^|{record_date}\n')
    if 'phone' in consumer_dict:
        pii_rows.append(f'{consumer_dict["id"]}|^|{dealer_id}|^|{consumer_dict["phone"]}|^|phone|^|{record_date}\n')

    address_updates = make_address_json(consumer_dict)
    if address_updates:
        pii_rows.append(f'{consumer_dict["id"]}|^|{dealer_id}|^|{address_updates}|^|address|^|{record_date}\n')

    return pii_rows


def get_new_consumers(last_executed: datetime, dealer_id: int):
    """Gets FD parsed list of new consumers PII."""
    with DBSession() as session:
        new_consumers: list[Consumer] = (session
                .query(Consumer)
                .filter(Consumer.dealer_id == dealer_id)
                .filter(Consumer.db_creation_date >= last_executed)
                .all())

    return [
        row for consumer in new_consumers
        for row in make_pii_rows(consumer.as_dict(), dealer_id)]


def get_updates(last_executed: datetime, dealer_id: int):
    """Gets FD parsed list of consumers updated PII."""
    with DBSession() as session:
        updated_consumers = (session
                .query(Consumer, AuditLog.new_values)
                .join(Consumer, AuditLog.row_id == Consumer.id)
                .filter(Consumer.dealer_id == dealer_id)
                .filter(AuditLog.table_name == 'cdpi_consumer')
                .filter(AuditLog.operation == 'UPDATE')
                .filter(AuditLog.db_update_date >= last_executed)
                .all())

    return [row for consumer, new_values in updated_consumers
            for row in make_pii_rows({ **consumer.as_dict(), **new_values }, dealer_id)]


def record_handler(record: SQSRecord):
    logger.info(f'Record: {record}')

    now = datetime.now(timezone.utc)
    last_executed = now - timedelta(days=1, minutes=30)  # add 30min as a buffer
    payload: dict = record.json_body
    dealer_id = payload['dealer_id']

    try:
        with ThreadPoolExecutor() as executor:
            future_new_consumers = executor.submit(get_new_consumers, last_executed, dealer_id)
            future_updated_consumers = executor.submit(get_updates, last_executed, dealer_id)

            # wait for both to complete and gather results
            new_consumers = future_new_consumers.result()
            updated_consumers = future_updated_consumers.result()

        filename = f'eid_pii_match{ "" if IS_PROD else "_test" }_impel_{dealer_id}_{now.strftime("%Y%m%d%H%M%S")}.txt'
        filepath = f'/tmp/{filename}'
        with open(filepath, mode='w', newline='') as file:
            file.write(FD_FILE_HEADER)
            file.writelines(new_consumers + updated_consumers)

        key = f'fd-pii-outbound/{now.year}/{now.month}/{now.day}/{filename}'
        s3_client.upload_file(Filename=filepath, Bucket=SHARED_BUCKET, Key=key)
        logger.info(f'Uploaded PII file to {key}')
    except Exception as e:
        logger.exception(f'Failed to generate PII file for {dealer_id} : {e}')
        raise


def lambda_handler(event: Any, context: Any):
    """Generate PII file for each dealer."""
    logger.info(f'Event: {event}')

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
        logger.exception(f"Error processing records: {e}")
        raise
