import logging
from os import environ
from dms_orm.session_config import DBSession
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
import boto3
from json import dumps


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

INVOKE_QUEUE = environ.get('INVOKE_QUEUE')
SQS = boto3.client('sqs')

def lambda_handler(event, _):
    logger.info(f'Event: {event}')

    try:
        with DBSession() as session:
            dealers = session.query(
                Dealer.id
            ).join(
                DealerIntegrationPartner, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
            ).filter(
                DealerIntegrationPartner.is_active == True,
                IntegrationPartner.id == 109    # change it for the actual quiter production value
            ).all()

        for dealer in dealers:
            SQS.send_message(
                QueueUrl=INVOKE_QUEUE,
                MessageBody=dumps({'dealer_id': dealer[0]})
            )
    except Exception as e:
        logger.exception(f'Error on GetActiveDealers: {str(e)}')
        raise
