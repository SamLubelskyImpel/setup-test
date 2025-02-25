"""
Reprocess events from an SQS lambda handler
Usage: python sqs_events.py -e {ENVIRONMENT} -l {LOG GROUP NAME} --start {START DATETIME} --end {END DATETIME}
"""

import boto3
from argparse import ArgumentParser
from datetime import datetime
import calendar
from json import loads, dumps

PROFILES = {
    'test': 'unified-test',
    'prod': 'devadmin'
}

ACCOUNTS = {
    'test': '143813444726',
    'prod': '196800776222'
}

parser = ArgumentParser(description='Reprocess SQS events processed by lambda')
parser.add_argument('-e', '--environment', choices=['test', 'prod'], required=True, help='Environment to reprocess events')
parser.add_argument('-l', '--log', required=True, help='Log group name')
parser.add_argument('--start', required=True, help='Start UTC datetime to reprocess')
parser.add_argument('--end', required=True, help='End UTC datetime to reprocess')

args = parser.parse_args()
environment = args.environment
log_group = args.log
date_start = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
date_end = datetime.strptime(args.end, '%Y-%m-%dT%H:%M:%S')

boto3.setup_default_session(profile_name=PROFILES[environment])
logs = boto3.client('logs')
sqs = boto3.client('sqs')

print(
    '----> Starting\n'
    f'Log group: {log_group}\n'
    f'From: {date_start}\n'
    f'To: {date_end}'
)

# Get the error execution IDs first
response = logs.filter_log_events(
    logGroupName=log_group,
    startTime=calendar.timegm(date_start.timetuple())*1000,
    endTime=calendar.timegm(date_end.timetuple())*1000,
    filterPattern='"[ERROR]"'
)

error_execution_ids = [event['message'].split()[2] for event in response['events']]
next_token = response.get('nextToken', None)

while True:
    if not next_token:
        break

    response = logs.filter_log_events(
        logGroupName= log_group,
        startTime=calendar.timegm(date_start.timetuple())*1000,
        endTime=calendar.timegm(date_end.timetuple())*1000,
        filterPattern='"[ERROR]"',
        nextToken=next_token
    )
    error_execution_ids.extend([event['message'].split()[2] for event in response['events']])
    next_token = response.get('nextToken', None)

print(f'Found {len(error_execution_ids)} error messages')

events = {}

# Find the event for each execution ID
for execution_id in error_execution_ids:
    if execution_id == 'All':   # This might happen due to powertools batcher logging
        continue

    response = logs.filter_log_events(
        logGroupName=log_group,
        startTime=calendar.timegm(date_start.timetuple())*1000,
        endTime=calendar.timegm(date_end.timetuple())*1000,
        filterPattern=f'"{execution_id}" "Event:"'
    )

    if not len(response['events']):
        print(f'Event not found for executionId {execution_id}')
        continue

    event_message = response['events'][0]['message']
    try:
        event = ''.join(event_message.split()[4:]).replace("\'", "\"").replace('None', 'null').replace('False', 'false').replace('True', 'true').replace("\"{", "{").replace("}\"", "}")
        event_dict = loads(event)
        for event_record in event_dict['Records']:
            # Lambda retries failed SQS events, this assures we only get 1 per messageId
            events[event_record['messageId']] = (execution_id, event_record)
    except:
        print(f'Failed to parse event {execution_id}')

events = events.values()
print(f'Found {len(events)} events to reprocess')

# Redrive SQS with the detected events
for execution_id, event in events:
    queue_arn = event['eventSourceARN']
    body = event['body']

    try:
        split_arn = queue_arn.split(':')
        queue_name = split_arn[-1]
        account_id = split_arn[-2]
        aws_region = split_arn[-3]
        queue_url = f"https://sqs.{aws_region}.amazonaws.com/{account_id}/{queue_name}"

        print(
            f'----> Reprocessing {execution_id}\n'
            f'Queue: {queue_url}'
        )

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(body)
        )
    except Exception as err:
        print(f'Error: {err}')
