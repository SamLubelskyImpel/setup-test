"""
Reprocess events from an API
Usage: python api_events.py -e {ENVIRONMENT} -l {LOG GROUP NAME} --start {START DATETIME} --end {END DATETIME}
"""

import boto3
from argparse import ArgumentParser
from datetime import datetime
import calendar
from json import loads
import requests

PROFILES = {
    'test': 'unified-test',
    'prod': 'devadmin'
}

ACCOUNTS = {
    'test': '143813444726',
    'prod': '196800776222'
}

parser = ArgumentParser(description='Reprocess events from shared layer APIs')
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

events = []

# Find the API event for each execution ID
for execution_id in error_execution_ids:
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
    event = ''.join(event_message.split()[4:]).replace("\'", "\"").replace('None', 'null').replace('False', 'false').replace('True', 'true').replace("\"{", "{").replace("}\"", "}")
    event_dict = loads(event)
    events.append((execution_id, event_dict))

print(f'Found {len(events)} events to reprocess')

# Call the API with the detected events
for execution_id, event in events:
    method = event['httpMethod']
    url = event['requestContext']['domainName'] + event['requestContext']['path']
    partner_id = event['headers']['partner_id']
    api_key = event['headers']['x_api_key']
    body = event['body']

    try:
        print(
            f'----> Reprocessing {execution_id}\n'
            f'Endpoint: {method} {url}'
        )

        res = requests.request(
            method=method,
            url='https://' + url,
            headers={
                'partner_id': partner_id,
                'x_api_key': api_key
            },
            json=body
        )

        print(f'Response: {res.status_code} {res.text}')
    except Exception as err:
        print(f'Error: {err}')
