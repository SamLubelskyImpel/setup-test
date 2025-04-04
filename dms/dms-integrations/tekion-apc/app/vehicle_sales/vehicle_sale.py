"""Tekion deals API call."""
import logging
import boto3
from os import environ
from json import loads, dumps
from uuid import uuid4
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from tekion_wrapper import TekionWrapper
from datetime import datetime, timezone, timedelta


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
eventbridge_client = boto3.client('scheduler')

BATCH_SIZE = int(environ.get("BATCH_SIZE"))


def create_schedules(s3_keys):
    last_schedule = datetime.now()
    for key in s3_keys:
        last_schedule += timedelta(minutes=15)

        process_queue = environ.get('PROCESS_DEALS_QUEUE')
        schedule_dlq = environ.get('SCHEDULER_DLQ')
        scheduler_role = environ.get('SCHEDULER_ROLE')
        body = dumps({
            "s3_key": key
        })

        schedule_name = f"tekion_process_deal_schedule_{last_schedule.strftime('%Y_%m_%d_%H_%M_%S')}"

        schedule_arn = eventbridge_client.create_schedule(
            ActionAfterCompletion='DELETE',
            FlexibleTimeWindow = {
                'Mode': 'OFF'
            },
            Name = schedule_name,
            ScheduleExpression= f"at({last_schedule.strftime('%Y-%m-%dT%H:%M:%S')})",
            Target={
                'Arn': process_queue,
                'DeadLetterConfig': {
                    'Arn': schedule_dlq
                },
                'Input': body,
                'RoleArn': scheduler_role
            }
        )

        logger.info(f"Eventbridge Schedule created with ARN {schedule_arn['ScheduleArn']}") 

def chunk_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def save_chunk(data, tekion_wrapper):
    
    now = tekion_wrapper.end_dt
    filename = f'fi_closed_deal_{str(uuid4())}.json'
    key = f'tekion-apc/landing-zone/fi_closed_deal/{tekion_wrapper.dealer_id}/{now.year}/{now.month}/{now.day}/{now.isoformat()}_{filename}'
    tekion_wrapper.upload_data(
        data, key
    )
    return key

def parse_data(record: SQSRecord):
    """Parse and handle SQS Message."""
    logger.info(record)

    data = record.json_body

    tekion_wrapper = TekionWrapper(
        dealer_id=data["dealer_id"],
        end_dt_str=data["end_dt_str"]
    )

    api_data = tekion_wrapper.get_deals()

    for deal in api_data:
        deal.update({"dms_id": tekion_wrapper.dealer_id})

    s3_keys = []
    chunked_list = chunk_list(api_data, BATCH_SIZE)

    for chunk in chunked_list:
        s3_keys.append(save_chunk(chunk, tekion_wrapper))

    create_schedules(s3_keys)
        
def lambda_handler(event, context):
    """Query Tekion deals API."""
    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=parse_data,
            processor=processor,
            context=context
        )
        return result
    except:
        logger.exception(f"Error processing batch")
        raise
