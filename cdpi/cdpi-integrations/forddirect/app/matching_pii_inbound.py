import logging
import os
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from typing import Any, List, Dict
import boto3
from json import loads
from sqlalchemy.orm import Session
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer_profile import ConsumerProfile
import urllib.parse
from sqlalchemy.dialects.postgresql import insert
from cdpi_orm.models.integration_partner import IntegrationPartner
from sqlalchemy.exc import SQLAlchemyError

# Set up logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

# Initialize S3 client
s3 = boto3.client('s3')

FORD_DIRECT_SUCCESS_STATUS = '1'


def fetch_s3_file(bucket_name: str, file_key: str) -> List[str]:
    """Download the S3 file and return its contents as lines."""
    decoded_key = urllib.parse.unquote(file_key)
    download_path = f'/tmp/{os.path.basename(decoded_key)}'
    s3.download_file(bucket_name, decoded_key, download_path)
    logger.info(f'Downloaded file from S3: {download_path}')

    # Read file contents as list of rows
    with open(download_path, 'r') as f:
        return f.readlines()


def parse_pii_file(file_lines: List[str]) -> List[Dict[str, Any]]:
    """Parse the PII file into a list of dictionaries (for each consumer row)."""
    header, *data_lines = file_lines
    fields = header.strip().split("|^|")
    fields = [i.lower() for i in fields]

    parsed_rows = []
    for line in data_lines:
        line_data = line.strip().split("|^|")
        row = dict(zip(fields, line_data))

        if row.get('match_result') == FORD_DIRECT_SUCCESS_STATUS:
            parsed_rows.append(row)

    return parsed_rows


def merge_consumer_rows(pii_rows):
    """Merge multiple PII rows into a single consumer dictionary per ext_consumer_id."""
    merged_data = {}
    for row in pii_rows:
        ext_consumer_id = row['ext_consumer_id']
        if ext_consumer_id not in merged_data:
            merged_data[ext_consumer_id] = {
                'ext_consumer_id': ext_consumer_id,
                'dealer_identifier': row['dealer_identifier'],
                'fdguid': row['fdguid'],
                'fddguid': row['fddguid'],
                'match_result': row['match_result'],
            }
        else:
            # Check for FDGUID consistency and log if there's a mismatch
            if row['fdguid'] != merged_data[ext_consumer_id]['fdguid']:
                logger.warning(
                    f"Multiple FDGUIDs detected for {ext_consumer_id}. "
                    f"Using {merged_data[ext_consumer_id]['fdguid']}, ignoring {row['fdguid']}"
                )

    return merged_data


def get_ford_direct_partner_id(session: Session) -> int:
    """Fetch the integration partner ID for Ford Direct."""
    integration_partner = session.query(IntegrationPartner).filter_by(
        impel_integration_partner_name='FORD_DIRECT'
    ).one_or_none()

    if not integration_partner:
        logger.error("Integration partner 'FORD_DIRECT' not found")
        raise ValueError("Integration partner 'FORD_DIRECT' not found")

    return integration_partner.id


def upsert_consumer_profile(merged_data: Dict[str, Any], session: Session):
    """Upsert the consumer profiles into the database."""
    try:
        integration_partner_id = get_ford_direct_partner_id(session)

        for consumer_id, consumer_data in merged_data.items():

            insert_stmt = insert(ConsumerProfile).values(
                consumer_id=consumer_id,
                integration_partner_id=integration_partner_id,
                cdp_master_consumer_id=consumer_data.get('fdguid'),
                cdp_dealer_consumer_id=consumer_data.get('fddguid')
            )

            # Define the update operation in case of conflict
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['consumer_id', 'integration_partner_id'],
                set_={
                    'cdp_master_consumer_id': consumer_data.get('fdguid'),
                    'cdp_dealer_consumer_id': consumer_data.get('fddguid')
                }
            )

            session.execute(update_stmt)

        session.commit()

    except SQLAlchemyError as e:
        logger.exception(f"Database operation failed: {str(e)}")
        session.rollback()
        raise
    except ValueError as ve:
        logger.exception(f"Failed to retrieve Ford Direct ID: {str(ve)}")
        raise


def record_handler(record: Dict[str, Any]):
    """Process an individual record from the SQS event."""
    logger.info(f'Record: {record}')

    try:
        event = loads(record['body'])
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']

        # Fetch and parse S3 file
        file_lines = fetch_s3_file(bucket_name, file_key)
        pii_rows = parse_pii_file(file_lines)
        logger.info(f'Total successfully matched rows: {len(pii_rows)}')

        # Merge rows by consumer ID
        merged_data = merge_consumer_rows(pii_rows)
        logger.info(f'Total unique consumers: {len(merged_data)}')

        # Upsert into the database
        with DBSession() as session:
            upsert_consumer_profile(merged_data, session)

        logger.info(f'Successfully processed file: {file_key}')

    except Exception as e:
        logger.exception(f'Error processing record: {str(e)}')
        raise


def lambda_handler(event: Any, context: Any):
    """Main Lambda handler function for processing SQS events."""
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
        logger.exception(f"Error processing event: {str(e)}")
        raise
