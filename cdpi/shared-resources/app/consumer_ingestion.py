import boto3
from os import environ
import logging
import urllib.parse
import csv
from json import loads
from io import StringIO
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

LOG_LEVEL = environ.get('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')

PRODUCT_MAPPING = {
    "sales-ai": "Sales AI",
    "service-ai": "Service AI"
}

FIELD_MAPPINGS = {
    "source_consumer_id": "customer_id",
    "first_name": "first_name",
    "last_name": "last_name",
    "phone": "phone_number",
    "email": "email_address",
    "email_optin_flag": "email_optin_flag",
    "phone_optin_flag": "phone_optin_flag",
    "sms_optin_flag": "sms_optin_flag",
    "address_line_1": "address_1",
    "address_line_2": "address_2",
    "suite": "suite",
    "city": "city",
    "areatype": "areatype",
    "area": "area",
    "country": "country",
    "zip": "zip",
    "zipextra": "zipextra",
    "pobox": "pobox"
}


def parse(csv_object):
    """Parse CSV object and extract entries"""
    csv_reader = csv.DictReader(StringIO(csv_object))
    entries = []

    for row in csv_reader:
        product_dealer_id = row.get('dealer_id')
        sfdc_account_id = row.get('salesforce_id')
        entry = {}

        for cdpi_field, inbound_data_field in FIELD_MAPPINGS.items():
            if cdpi_field in ('email_optin_flag', 'phone_optin_flag', 'sms_optin_flag'):
                value = row.get(inbound_data_field, None)
                if value:
                    entry[cdpi_field] = value.lower() == 'true'
            else:
                entry[cdpi_field] = row.get(inbound_data_field, None)

        # # Remove null values from entry
        # keys_to_remove = [key for key, value in entry.items() if value in (None, '')]
        # for key in keys_to_remove:
        #     del entry[key]

        entries.append(entry)

    if not product_dealer_id or not sfdc_account_id:
        logger.error('product_dealer_id or sfdc_account_id not found in the CSV')
        raise

    return entries, product_dealer_id, sfdc_account_id


def write_to_rds(entries, product_name, product_dealer_id, sfdc_account_id):
    """Write consumer identities to RDS"""
    with DBSession() as session:
        try:
            # Identify product
            db_product = session.query(
                Product.id
            ).filter(
                Product.product_name == product_name
            ).first()
            if not db_product:
                logger.error(f"Product {product_name} not found")
                return

            # Identity dealer
            dealer_query = session.query(
                Dealer.id,
            ).filter(
                Dealer.sfdc_account_id == sfdc_account_id
            )
            if product_name == "Sales AI":
                dealer_query = dealer_query.filter(Dealer.salesai_dealer_id == product_dealer_id)
            elif product_name == "Service AI":
                dealer_query = dealer_query.filter(Dealer.serviceai_dealer_id == product_dealer_id)

            db_dealer = dealer_query.first()
            if not db_dealer:
                logger.error(f"Dealer {product_dealer_id} not found for product {product_name}")
                return

            for entry in entries:
                logger.info(f"Adding consumer: {entry}")
                # Create consumer
                insert_stmt = insert(Consumer).values(
                    dealer_id=db_dealer.id,
                    product_id=db_product.id,
                    **entry
                )
                entry.pop('source_consumer_id')
                update_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=['dealer_id', 'product_id', 'source_consumer_id'],
                    set_={
                        **entry,
                    }
                )
                session.execute(update_stmt)

            session.commit()

        except SQLAlchemyError as e:
            # Rollback in case of any error
            session.rollback()
            logger.info(f"Error occurred during database operations: {e}")
            raise e

    logger.info("Consumers added to the database")


def record_handler(record: SQSRecord):
    """Process CSV file from S3 and write to RDS"""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        bucket_name = message["detail"]["bucket"]["name"]
        file_key = message["detail"]["object"]["key"]
        decoded_key = urllib.parse.unquote(file_key)

        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=decoded_key
        )
        csv_object = csv_file['Body'].read().decode('utf-8')
        product = decoded_key.split('/')[1]
        product_name = PRODUCT_MAPPING.get(product)
        if not product_name:
            logger.error(f"Product {product} not found in PRODUCT_MAPPING")
            return

        entries, product_dealer_id, sfdc_account_id = parse(csv_object)
        write_to_rds(entries, product_name, product_dealer_id, sfdc_account_id)
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
