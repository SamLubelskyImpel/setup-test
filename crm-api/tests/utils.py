import boto3


WBNS_LOG_GROUP = '/aws/lambda/wbns-monitoring-test'


def get_wbns_log_events():
    client = boto3.client('logs')

    response = client.describe_log_streams(
        logGroupName=WBNS_LOG_GROUP,
        orderBy='LastEventTime',
        descending=True,
        limit=1
    )

    log_streams = response['logStreams']
    if not log_streams:
        raise ValueError("No log streams found")

    response = client.get_log_events(
        logGroupName=WBNS_LOG_GROUP,
        logStreamName=log_streams[0]['logStreamName'],
        limit=20  # Adjust as needed
    )

    return [event['message'] for event in response['events']]
