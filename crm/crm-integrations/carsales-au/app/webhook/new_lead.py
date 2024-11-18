import logging
from os import environ
from typing import Any
from json import loads, dumps
from sqlalchemy import or_, and_
from datetime import datetime, timezone
from uuid import uuid4
import boto3
import requests


logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())

INTEGRATIONS_BUCKET = environ.get('INTEGRATIONS_BUCKET')
REPORTING_TOPIC_ARN = environ.get('REPORTING_TOPIC_ARN')
ENVIRONMENT = environ.get('ENVIRONMENT')
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")

S3_CLIENT = boto3.client('s3')
SNS_CLIENT = boto3.client('sns')
SECRET_CLIENT = boto3.client("secretsmanager")


class DealershipNotActive(Exception):
    pass


def save_raw_event(partner_name: str, dealer: dict, body: dict):
    s3_product_prefix = 'carsales-au' if partner_name == 'CARSALES_AU' else 'carsales-dealersocket-au'
    now = datetime.now(tz=timezone.utc)
    s3_key = f'raw/{s3_product_prefix}/{dealer["product_dealer_id"]}/{now.year}/{now.month}/{now.day}/{now.hour}/{now.minute}_{str(uuid4())}.json'
    # set our crm_dealer_id on the lead object
    body['crm_dealer_id'] = dealer['crm_dealer_id']
    S3_CLIENT.put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=s3_key,
        Body=dumps(body)
    )
    logger.info(f'Raw event saved to {s3_key}')


def log_event(event: dict):
    del event['headers']
    del event['multiValueHeaders']
    logger.info(f'Event: {event}')


def send_alert(message: str):
    SNS_CLIENT.publish(
        TopicArn=REPORTING_TOPIC_ARN,
        Message=message,
        Subject='CarSales AU CRM: Lead Rejected - Dealer not found'
    )


def get_secrets():
    """Get CRM API secrets."""
    secret = SECRET_CLIENT.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret['SecretString'])['impel']
    secret_data = loads(secret)
    return 'impel', secret_data["api_key"]


def get_dealers(integration_partner_name: str):
    partner_id, api_key = get_secrets()

    url = f'https://{CRM_API_DOMAIN}/dealers'
    params = {'integration_partner_name': integration_partner_name}

    logger.info(f'Request to {url} with {params}')

    response = requests.get(
        url=url,
        headers={'partner_id': partner_id, 'x_api_key': api_key},
        params=params,
    )

    logger.info(f'{url} responded with: {response.status_code} {response.text}')

    if response.status_code != 200:
        logger.error(
            f'Error getting dealers {integration_partner_name}: {response.text}'
        )
        raise

    dealers = response.json()

    # Filter by active Sales AI dealers
    dealers = list(filter(lambda dealer: dealer.get('is_active_salesai', False), dealers))
    return dealers


def lambda_handler(event: dict, context: Any):
    log_event(event)

    body = event['body']
    dict_body: dict = loads(body)
    crm_dealer_id = dict_body['SellerIdentifier']

    try:
        dealers = get_dealers('CARSALES_AU|DEALERSOCKET_AU')

        partner_name = 'CARSALES_AU'
        dealer = next(iter([d for d in dealers if d['crm_dealer_id'] == crm_dealer_id]), None)

        if not dealer:
            partner_name = 'DEALERSOCKET_AU'
            dealer = next(iter([
                d for d in dealers
                if d.get('metadata', {}).get('lead_vendor') == 'carsales'
                and d.get('metadata', {}).get('carsales_dealer_id') == crm_dealer_id
            ]), None)

        if not dealer:
            raise DealershipNotActive()

        logger.info(f'Found active dealership {dealer}')
        save_raw_event(partner_name, dealer, dict_body)

        return {
            'statusCode': 200
        }
    except DealershipNotActive:
        message = f'Dealership not active for crm_dealer_id = {crm_dealer_id}'
        logger.warning(message)
        send_alert(message)

        return {
            'statusCode': 400,
            'body': dumps({ 'error': 'Dealership not active' })
        }
    except:
        logger.exception('Error processing new lead')
        raise
