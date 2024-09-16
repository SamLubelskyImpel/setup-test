import boto3


INS_LOG_GROUP = '/aws/lambda/notification-service-test-MonitoringLambda'

unified_session = boto3.session.Session(profile_name='unified-test')


def get_ins_log_events():
    client = unified_session.client('logs')

    response = client.describe_log_streams(
        logGroupName=INS_LOG_GROUP,
        orderBy='LastEventTime',
        descending=True,
        limit=1
    )

    log_streams = response['logStreams']
    if not log_streams:
        raise ValueError("No log streams found")

    response = client.get_log_events(
        logGroupName=INS_LOG_GROUP,
        logStreamName=log_streams[0]['logStreamName'],
        limit=20  # Adjust as needed
    )

    return [event['message'] for event in response['events']]
