import logging
from os import environ
import boto3


LOG_LEVEL = environ.get("LOG_LEVEL", "INFO")
TOPIC_ARN = environ.get("CE_SNS_TOPIC_ARN")
REPROCESS_QUEUES = [
    environ.get("SERVICE_FILE_DLQ"),
    environ.get("SALES_FILE_DLQ"),
]

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
sqs = boto3.client("sqs")
sns = boto3.client("sns")


def send_alert(message: str):
    sns.publish(
        TopicArn=TOPIC_ARN,
        Message=message,
        Subject="[CDPI Shared] Error Reprocessing Events",
    )


def lambda_handler(event, context):
    logger.info(f"Event: {event}")
    for queue in REPROCESS_QUEUES:
        try:
            sqs.start_message_move_task(SourceArn=queue, MaxNumberOfMessagesPerSecond=1)
            logger.info(f"Started redrive on {queue}")
        except Exception as e:
            message = f"Failed redrive on {queue}: {e}"
            logger.exception(message)
            send_alert(message)
