import logging
import os
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from datetime import datetime, timezone, timedelta
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product
from cdpi_orm.models.consumer_profile import ConsumerProfile
import csv
import boto3


logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

s3_client = boto3.client('s3')

SHARED_BUCKET = os.environ.get('SHARED_BUCKET')
HEADERS = ['dealer_id', 'customer_id', 'salesforce_id', 'esm_score', 'epm_score']

def record_handler(record: SQSRecord):
    logger.info(f'Record: {record}')

    now = datetime.now(timezone.utc)
    last_executed = now - timedelta(minutes=6)  # adding 1min as a buffer
    payload: dict = record.json_body
    dealer_id = payload['dealer_id']

    rows_per_product = {
        'salesai': [],
        'serviceai': []
    }

    try:
        with DBSession() as session:
            scores: list[tuple[Product, ConsumerProfile, Dealer, str]] = (session.query(Product, ConsumerProfile, Dealer, Consumer.source_consumer_id)
                      .join(Consumer, ConsumerProfile.consumer_id == Consumer.id)
                      .join(Product, Consumer.product_id == Product.id)
                      .join(Dealer, Consumer.dealer_id == Dealer.id)
                      .filter(Dealer.id == dealer_id)
                      .filter(ConsumerProfile.score_update_date >= last_executed)
                      .all())

        for product, consumer_profile, dealer, consumer_id in scores:
            product_name = 'salesai' if product.product_name == 'Sales AI' else 'serviceai'
            if not product_name:
                logger.warning(f'Product not supported {product.product_name}')
                continue

            product_dealer_id = dealer.salesai_dealer_id if product_name == 'salesai' else dealer.serviceai_dealer_id
            rows_per_product[product_name].append((
                product_dealer_id,
                consumer_id,
                dealer.sfdc_account_id,
                consumer_profile.dealer_pscore_service if product_name == 'serviceai' else None,
                consumer_profile.dealer_pscore_sales if product_name == 'salesai' else None
            ))

        for product, scores in rows_per_product.items():
            if not (len(scores)):
                logger.warning(f'No scores for {(product, dealer_id)}')
                continue

            filename = f'{scores[0][2]}_{now.isoformat()}.csv'
            with open(f'/tmp/{filename}', mode='w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(HEADERS)
                writer.writerows(scores)

            s3_key = f'scores-syndication/{product}/{now.year}/{now.month}/{now.day}/{dealer_id}/{filename}'
            s3_client.upload_file(
                f'/tmp/{filename}',
                SHARED_BUCKET,
                s3_key)
            logger.info(f'Uploaded scores file to {s3_key}')
    except:
        logger.exception(f'Failed to generate scores file for {dealer_id}')
        raise


def lambda_handler(event, context):
    """Generate scores files for each dealer and product."""
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
    except:
        logger.exception(f"Error processing records")
        raise