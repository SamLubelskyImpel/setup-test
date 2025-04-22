from json import dumps
import boto3
from os import environ
from datetime import datetime

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


def model_to_dict(instance, rename_id_to=None, metadata_attr="metadata_"):
    """
    Convert a SQLAlchemy model instance into a serializable dict.
    
    - Formats datetime fields as ISO strings
    - Optionally renames the 'id' field
    - Removes SQLAlchemy metadata and inclues metadata_
    """
    result = {}

    for column in instance.__table__.columns:
        attr_name = column.key
        column_name = column.name
        value = getattr(instance, attr_name)

        if isinstance(value, datetime):
            result[column_name] = value.isoformat()
        else:
            result[column_name] = value

    result.pop("metadata", None)
    if hasattr(instance, metadata_attr):
        result["metadata"] = getattr(instance, metadata_attr)

    if rename_id_to:
        result[rename_id_to] = result.pop("id")

    return result