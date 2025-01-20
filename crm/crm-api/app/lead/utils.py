from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from os import environ
import boto3
from json import dumps

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

def get_restricted_query(session, integration_partner):
    query = session.query(Lead).join(Lead.consumer)

    if integration_partner:
        query = (
            query.join(Consumer.dealer_integration_partner)
                 .join(DealerIntegrationPartner.integration_partner)
                 .filter(IntegrationPartner.impel_integration_partner_name == integration_partner)
        )

    return query


def send_alert_notification(message, subject) -> None:
    """Send alert notification to CE team."""
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps({"message": message})}),
        Subject=f'CRM API: {subject}',
        MessageStructure='json'
    )