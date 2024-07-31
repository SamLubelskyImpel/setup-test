import boto3
from datetime import datetime, timezone
import json
from uuid import uuid4

client = boto3.client('events')

response = client.put_events(
    Entries=[
        {
            'Time': datetime.now(timezone.utc),
            'Source': 'test',
            'DetailType': 'string',
            'Detail': json.dumps({
                'events': [
                    {
                        'event_id': str(uuid4()),
                        'event_type': 'integrations.crm.salesai.lead.created',
                        'partner_name': 'TESTCRM',
                        'event_content': {
                            'message': 'Sales AI: Lead Created',
                            'lead_id': 123,
                            'customer_id': 456,
                            'dealer_id': 'test-dealer-id',
                            'created_ts': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
                        }
                    }
                ]
            }),
            'EventBusName': 'wbns-bus-test'
        }
    ]
)

print(response)