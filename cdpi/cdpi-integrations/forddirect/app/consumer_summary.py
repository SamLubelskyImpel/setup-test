import boto3
from os import environ
import logging
import urllib.parse
import csv
from io import StringIO
from json import loads
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer_profile import ConsumerProfile
from cdpi_orm.models.integration_partner import IntegrationPartner
from sqlalchemy.exc import SQLAlchemyError

LOG_LEVEL = environ.get('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')

FIELD_MAPPINGS = {
    "cdp_dealer_consumer_id": "c_fddguid",
    "dealer_pscore_sales": "epm_sales_decile",
    "dealer_pscore_service": "esm_service_decile",
    "consumer_id": "EXT_CONSUMER_ID"
}


def parse(csv_object):
    """Parse CSV object and extract entries"""
    csv_reader = csv.DictReader(StringIO(csv_object))
    entries = []

    for row in csv_reader:
        entry = {}

        for cdpi_field, inbound_data_field in FIELD_MAPPINGS.items():
            entry[cdpi_field] = row.get(inbound_data_field, None)

        # Remove null values from entry
        keys_to_remove = [key for key, value in entry.items() if value in (None, '')]
        for key in keys_to_remove:
            del entry[key]

        entries.append(entry)

    return entries


def write_to_rds(entries):
    """Write consumer identities to RDS"""
    with DBSession() as session:
        try:
            # Identify integration partner
            db_integration_partner = session.query(
                IntegrationPartner
            ).filter(
                IntegrationPartner.impel_integration_partner_name == "FORD_DIRECT"
            ).scalar()
            if not db_integration_partner:
                logger.error("Integration partner FORD_DIRECT not found")
                return

            for entry in entries:
                logger.info(f"Processing consumer profile: {entry}")

                # Select consumer profile by consumer_id
                db_consumer_profile = session.query(
                    ConsumerProfile
                ).filter(
                    ConsumerProfile.consumer_id == entry['consumer_id'],
                    ConsumerProfile.integration_partner_id == db_integration_partner.id
                ).first()

                if not db_consumer_profile:
                    logger.error(f"Consumer profile not found for consumer_id: {entry['consumer_id']}. Skipping.")
                    continue

                is_updated = False

                # Update sales score
                new_sales_score = entry.get('dealer_pscore_sales')
                if new_sales_score and db_consumer_profile.dealer_pscore_sales != new_sales_score:
                    db_consumer_profile.prev_dealer_pscore_sales = db_consumer_profile.dealer_pscore_sales
                    db_consumer_profile.dealer_pscore_sales = new_sales_score
                    db_consumer_profile.score_update_date = datetime.now()
                    is_updated = True

                # Update service score
                new_service_score = entry.get('dealer_pscore_service')
                if new_service_score and db_consumer_profile.dealer_pscore_service != new_service_score:
                    db_consumer_profile.prev_dealer_pscore_service = db_consumer_profile.dealer_pscore_service
                    db_consumer_profile.dealer_pscore_service = new_service_score
                    db_consumer_profile.score_update_date = datetime.now()
                    is_updated = True

                # Update CDP Dealer Consumer ID
                new_cdp_dealer_consumer_id = entry.get('cdp_dealer_consumer_id')
                if new_cdp_dealer_consumer_id and db_consumer_profile.cdp_dealer_consumer_id != new_cdp_dealer_consumer_id:
                    db_consumer_profile.cdp_dealer_consumer_id = new_cdp_dealer_consumer_id
                    is_updated = True

                # Only add the profile to the session if changes are made
                if is_updated:
                    session.add(db_consumer_profile)
                    logger.info(f"Consumer profile updated for consumer_id: {entry['consumer_id']}")
                else:
                    logger.info(f"Consumer profile unchanged for consumer_id: {entry['consumer_id']}")

            session.commit()

        except SQLAlchemyError as e:
            # Rollback in case of any error
            session.rollback()
            logger.error(f"Error occurred during database operations: {e}")
            raise e

    logger.info("Consumers added to the database")


def record_handler(record: SQSRecord):
    """Process CSV file from S3 and write to RDS"""
    logger.info(f'Record: {record}')
    try:
        event = loads(record["body"])
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)

        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=decoded_key
        )
        csv_object = csv_file['Body'].read().decode('utf-8')
        entries = parse(csv_object)
        write_to_rds(entries)

    except Exception as e:
        logger.error(f'Error: {e}')
        raise


def lambda_handler(event, context):
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
