import json
import base64
import logging
from os import environ
import zlib
import boto3

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def decode_logs_event(logs_event):
    """Decode the Base64-encoded data."""
    encoded_data = logs_event['awslogs']['data']
    decoded_data = base64.b64decode(encoded_data)
    decompressed_data = zlib.decompress(decoded_data, zlib.MAX_WBITS | 16)

    logs_data = json.loads(decompressed_data)
    return logs_data


def process_log_entry(log_entry, log_stream):
    """Process log entry and send alert notification."""
    # Example: "[TEST ALERT] This is a test alert. [CONTENT] Traceback (most recent call last)..."
    log_message = log_entry.split("[TEST ALERT]")[1].strip()
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
        if "[TEST ALERT]" in log['message']:
            process_log_entry(log['message'], log_stream)


def send_alert_notification(alert_title, alert_body) -> None:
    """Send alert notification to CE team."""
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=alert_body,
        Subject=f'DealerPeak CRM - {alert_title}',
        MessageStructure='string',
        MessageAttributes={
            'alert_type': {
                'DataType': 'String',
                'StringValue': 'TEST ALERT'
            }
        }
    )
