"""Get active dealers."""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from datetime import datetime, timedelta

from crm_orm.models.crm_integration_partner import IntegrationPartner
from crm_orm.models.crm_dealer import Dealer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def lambda_handler(event: Any, context: Any) -> Any:
    """Get active dealers."""
    logger.info(f"Event: {event}")

    dealer_configs = []

    current_time = datetime.utcnow()
    start_time = (current_time - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        body = loads(event["body"])
        partner_name = body["impel_integration_partner_name"]

        queue_url = loads(
            s3_client.get_object(
                Bucket=BUCKET,
                Key=f"configurations/{partner_name}.json"
            )
        )["invoke_dealer_queue_url"]

        with DBSession() as session:
            crm_partner = session.query(
                    IntegrationPartner
                ).filter(
                    IntegrationPartner.impel_integration_partner_name == partner_name
                ).first()

            if not crm_partner:
                logger.error(f"Integration Partner not found {partner_name}")
                raise

            dealers = session.query(
                    Dealer
                ).filter(
                    Dealer.crm_integration_partner_id == crm_partner.id,
                    Dealer.is_active is True
                ).all()

            if not dealers:
                logger.error(f"No active dealers found for {partner_name}")
                raise

            for dealer in dealers:
                dealer_configs.append({
                    "dealer_id": dealer.id,
                    "product_dealer_id": dealer.product_dealer_id,
                    "crm_dealer_id": dealer.crm_dealer_id,
                    "start_time": start_time,
                    "end_time": end_time,
                })

        for dealer in dealer_configs:
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=dumps(dealer)
            )

    except Exception as e:
        logger.error(f"Error occured getting active dealers: {e}")
        raise
