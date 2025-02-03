from crm_orm.models.integration_partner import IntegrationPartner
import boto3
from json import dumps
from os import environ

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

def get_restricted_query(query, integration_partner):
    """Restrict query based on integration partner."""
    if integration_partner:
        query = query.filter(IntegrationPartner.impel_integration_partner_name == integration_partner)

    return query

def send_alert_notification(subject, message) -> None:
    """Send alert notification to CE team."""
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps({"message": message})}),
        Subject=subject,
        MessageStructure='json'
    )