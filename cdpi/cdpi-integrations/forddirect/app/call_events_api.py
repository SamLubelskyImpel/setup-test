import logging  
from os import environ
from json import dumps
import boto3
from json import loads
import requests
from datetime import datetime, timezone

secret_client = boto3.client("secretsmanager")

ENVIRONMENT = environ.get("ENVIRONMENT")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_token():
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/event-publishing-api"
    )
    key = 'impel' if ENVIRONMENT == 'prod' else 'test'

    secret = loads(loads(secret["SecretString"])[key])
    return key, secret['api_key']

def lambda_handler(event, context):
    event_type = event["event_type"]
    integration_partner_id = event["integration_partner_id"]
    consumer_id = event["consumer_id"]
    source_consumer_id = event["source_consumer_id"]
    dealer_id = event["dealer_id"]
    salesai_dealer_id = event["salesai_dealer_id"]
    serviceai_dealer_id = event["serviceai_dealer_id"]
    product_name = event["product_name"]

    logger.info("call events api starting")
    logger.info(f"Event: {event}")

    try:
        client_id,token = get_token()
        
        logger.info(f"client_id: {client_id}, token: {token}, integration_partner_id: {integration_partner_id}, consumer_id: {consumer_id}, source_consumer_id: {source_consumer_id}, dealer_id: {dealer_id}, product_name: {product_name}")

        product_name = 'sales_ai' if product_name == 'Sales AI' else 'service_ai'
        source_dealer_id = salesai_dealer_id if product_name == "sales_ai" else serviceai_dealer_id

        event_json = {
            "event_json": [
                {
                    "event_type": event_type,
                    "integration_partner": integration_partner_id,
                    "consumer_id": consumer_id,
                    "dealer_id": dealer_id,
                    "source_consumer_id": source_consumer_id,
                    "source_dealer_id": source_dealer_id,
                    "source_application": product_name,
                    "request_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            ],
            "event_source": "cdp_integration_layer"
        }

        logger.info(f"Event JSON: {event_json}")

        response = requests.post(
            f"{environ.get('EVENTS_API_URL')}/v1/events",
            headers={
                "client-id": client_id,
                "api-key": token,
            },
            json=event_json
        )

        if response.status_code != 200:
            raise Exception(response.text)
        
        logger.info(f"Event created successfully. Response: {response.text}")

    except Exception as e:
        logger.error(f"Failed to create event: {response.text}")
        
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "message": f"Failed to create event: {response.text}"
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    logger.info("call events api completed")

    return {
        "statusCode": 204,
        "body": dumps(
            {
                "message": "Success"
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }