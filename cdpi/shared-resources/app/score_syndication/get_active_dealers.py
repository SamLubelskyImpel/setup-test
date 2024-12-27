from aws_lambda_powertools.utilities.data_classes import EventBridgeEvent
from typing import Any
import logging
import os
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.dealer import Dealer
import boto3
from json import dumps


logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

sqs_client = boto3.client('sqs')

PULL_SCORES_QUEUE = os.environ.get('PULL_SCORES_QUEUE')


def lambda_handler(event: EventBridgeEvent, context: Any):
    """Get active dealers to invoke the score syndication."""
    logger.info(f'Event: {event}')

    with DBSession() as session:
        active_dealers = (session
                          .query(Dealer.id)
                          .join(DealerIntegrationPartner, DealerIntegrationPartner.dealer_id == Dealer.id)
                          .filter(DealerIntegrationPartner.is_active)
                          .all())
        logger.info(f'Active dealers: {active_dealers}')

    for dealer in active_dealers:
        sqs_client.send_message(
            QueueUrl=PULL_SCORES_QUEUE,
            MessageBody=dumps({ 'dealer_id': dealer[0] })
        )
