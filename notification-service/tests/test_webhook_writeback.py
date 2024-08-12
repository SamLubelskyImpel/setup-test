import boto3
from time import sleep
from utils import send_event


spincar_session = boto3.session.Session(profile_name='test')
ddb = spincar_session.client('dynamodb')


def test_event_sent_to_client_webhook(writeback_event):
    send_event(writeback_event)

    event_id = writeback_event['events'][0]['event_id']
    exp_attrs = {
        ':cid': {'S': 'ins'},
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
