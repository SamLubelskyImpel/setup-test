import logging
from os import environ
from typing import Any
from json import loads, dumps
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


class ValidationError(Exception):
    pass


def save_raw_event(partner_name: str, dealer: dict, body: dict):
    """Save raw event to S3."""
    s3_product_prefix = 'carsales-au' if partner_name == 'CARSALES_AU' else 'carsales-dealersocket-au'
    format_string = '%Y/%m/%d/%H/%M'
    date_key = datetime.now(tz=timezone.utc).strftime(format_string)

    s3_key = f'raw/{s3_product_prefix}/{dealer["product_dealer_id"]}/{date_key}_{uuid4()}.json'

    # set our crm_dealer_id on the lead object
    body['crm_dealer_id'] = dealer['crm_dealer_id']
    S3_CLIENT.put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=s3_key,
        Body=dumps(body)
    )
    logger.info(f'Raw event saved to {s3_key}')


def get_secrets():
    """Get CRM API secrets."""
    secret = SECRET_CLIENT.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret['SecretString'])['impel']
    secret_data = loads(secret)
    return 'impel', secret_data["api_key"]


def get_dealers(integration_partner_name: str):
    """Get active Sales AI dealers."""
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
    """Accept new lead from CarSales."""
    logger.info(f'Event: {event}')
    crm_lead_id = None
    crm_dealer_id = None

    try:
        body = event['body']
        dict_body: dict = loads(body)

        crm_lead_id = dict_body.get('Identifier', '')
        seller_object = dict_body.get('Seller', {})
        crm_dealer_id = seller_object.get('Identifier', '')

        if not crm_lead_id or not crm_dealer_id:
            raise ValidationError('Invalid request body. Identifier and Seller.Identifier are required.')

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

    except DealershipNotActive:
        logger.error("[SUPPORT ALERT] Lead Rejected [CONTENT] Dealership is not active for crm_dealer_id {}".format(
            crm_dealer_id)
        )
        return {
            'statusCode': 401,
            'body': dumps({'error': 'This request is unauthorized. Dealership not active in Impel\'s system.'})
        }
    except ValidationError as e:
        logger.error(f'Error validating request: {e}')
        return {
            'statusCode': 400,
            'body': dumps({'error': str(e)})
        }
    except Exception as e:
        logger.exception(f'Error processing new lead: {e}')
        logger.error("[SUPPORT ALERT] Failed to accept lead [CONTENT] CRMLeadId: {}\nCRMDealerId: {}\nTraceback: {}".format(
            crm_lead_id, crm_dealer_id, e)
        )
        return {
            'statusCode': 500,
            'body': dumps({'error': 'Internal Server Error. Please contact Impel support.'})
        }

    return {
        'statusCode': 200,
        'body': dumps({'message': 'Lead accepted.'})
    }
