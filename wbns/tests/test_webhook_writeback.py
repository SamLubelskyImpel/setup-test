import boto3
from datetime import datetime, timezone
import json
from time import sleep


unified_session = boto3.session.Session(profile_name='unified-test')
spincar_session = boto3.session.Session(profile_name='test')

eventbridge = unified_session.client('events')
ddb = spincar_session.client('dynamodb')


def test_event_sent_to_client_webhook(event):
    eventbridge.put_events(
        Entries=[
            {
                'Time': datetime.now(timezone.utc).isoformat(),
                'Source': 'com.impel.crm-api',
                'DetailType': 'JSON',
                'Detail': json.dumps(event),
                'EventBusName': f'wbns-test-EventBus'
            }
        ]
    )

    event_id = event['events'][0]['event_id']
    exp_attrs = {
        ':cid': {'S': 'wbns'},
        ':event_id': {'S': event_id}
    }

    attempts = 0
    event_sent = False

    while attempts < 6:
        sleep(5)
        resp = ddb.query(TableName='Event-Log-test',
                        KeyConditionExpression='clientID = :cid',
                        FilterExpression='contains(raw_received, :event_id)',
                        ExpressionAttributeValues=exp_attrs)
        event_sent = len(resp['Items']) > 0
        if event_sent: break
        attempts += 1

    assert event_sent