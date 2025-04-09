from json import dumps
import boto3
from os import environ

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")


def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"Inventory internal API: {endpoint} Failure Alert",
        MessageStructure="json",
    )