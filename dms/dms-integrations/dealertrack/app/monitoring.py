import json
import base64
import logging
from os import environ
import zlib
import boto3

REPORTING_TOPIC_ARN = environ.get("REPORTING_TOPIC_ARN")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def decode_logs_event(logs_event):
    """Decode the Base64-encoded data."""
    encoded_data = logs_event['awslogs']['data']
    decoded_data = base64.b64decode(encoded_data)
    decompressed_data = zlib.decompress(decoded_data, zlib.MAX_WBITS | 16)

    logs_data = json.loads(decompressed_data)
    return logs_data


def process_support_log(log_entry, log_stream):
    """Process support log and send alert notification."""
    # Example: "[SUPPORT ALERT] This is a test alert. [CONTENT] Traceback (most recent call last)..."
    log_message = log_entry.split("[SUPPORT ALERT]")[1].strip()
    alert, content = log_message.split("[CONTENT]")

    alert_body = content.strip() + f"\nLog Stream: {log_stream}"

    send_alert_notification(alert, alert_body)


def lambda_handler(event, context):
    """Process CloudWatch Logs event."""
    logger.info(f"Event: {event}")

    log_data = decode_logs_event(event)
    logger.info(f"Decoded Event: {log_data}")

    log_stream = log_data['logGroup'] + '/' + log_data['logStream']
    for log in log_data['logEvents']:
        if "[SUPPORT ALERT]" in log['message']:
            process_support_log(log['message'], log_stream)


def send_alert_notification(alert_title, alert_body, alert_type="SUPPORT ALERT") -> None:
    """Send alert notification to Support team."""
    logger.info(
        "Preparing to send alert notification: Title=%s, Body=%s, Type=%s",
        alert_title, alert_body, alert_type
    )

    sns_client = boto3.client('sns')
    try:
        response = sns_client.publish(
            TopicArn=REPORTING_TOPIC_ARN,
            Message=alert_body,
            Subject=f'DMS Shared Layer Alerts: Dealertrack - {alert_title}',
            MessageStructure='string'
        )
        logger.info("Alert notification sent successfully: %s", response)
    except Exception as e:
        logger.error(f"Failed to send alert notification: {e}")
