from datetime import datetime, timezone
import requests
from os import environ
from json import loads, dumps
import boto3
import logging

from cdpi_orm.models.audit_dsr import AuditDsr
from cdpi_orm.models.consumer_profile import ConsumerProfile

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
secret_client = boto3.client("secretsmanager")

def clear_consumer_profile(consumer_profile : ConsumerProfile):
    datetime_now = datetime.now(timezone.utc)

    consumer_profile.master_pscore_sales = None
    consumer_profile.master_pscore_service = None
    consumer_profile.dealer_pscore_sales = None
    consumer_profile.dealer_pscore_service = None
    consumer_profile.prev_master_pscore_sales = None
    consumer_profile.prev_master_pscore_service = None
    consumer_profile.prev_dealer_pscore_sales = None
    consumer_profile.prev_dealer_pscore_service = None
    consumer_profile.score_updated_date = datetime_now
    return consumer_profile

def create_audit_dsr(integration_partner_id, consumer_id, event_type, complete_date=None, complete_flag=False):
    datetime_now = datetime.now(timezone.utc)
    
    return AuditDsr(
        consumer_id=consumer_id,
        integration_partner_id=integration_partner_id,
        dsr_request_type=event_type,
        request_date=datetime_now,
        complete_flag=complete_flag,
        complete_date=complete_date,
    )

def get_token():
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/event-publishing-api"
    )
    key = 'impel' if ENVIRONMENT == 'prod' else 'test'

    secret = loads(loads(secret["SecretString"])[key])
    return key, secret['api_key']
    
def create_event(integration_partner_id, consumer, dealer, product, event_type):
    client_id,token = get_token()
    
    product_name = 'sales_ai' if product.product_name == 'Sales AI' else 'service_ai'
    source_dealer_id = dealer.salesai_dealer_id if product_name == "sales_ai" else dealer.serviceai_dealer_id

    requests.post(
        f"{environ.get('EVENTS_API_URL')}/v1/events",
        headers={
            "client-id": client_id,
            "api-key": token,
        },
        json={
            "event_json": [
                {
                    "event_type": event_type,
                    "integration_partner": integration_partner_id,
                    "consumer_id": consumer.id,
                    "dealer_id": dealer.id,
                    "source_consumer_id": consumer.source_consumer_id,
                    "source_dealer_id": source_dealer_id,
                    "source_application": product_name,
                    "request_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            ],
            "event_source": "cdp_integration_layer"
        }
    )

def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"CDPI FORD DIRECT: {endpoint} Failure Alert",
        MessageStructure="json",
    )