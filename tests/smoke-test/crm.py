from utils import test_endpoint, BaseSmokeTest
from consts import ENV
import boto3
from json import loads
from datetime import datetime, timezone, timedelta


class CRMSmokeTest(BaseSmokeTest):
    def __init__(self):
        super().__init__()

        api_secret_name = f'{ENV}/crm-api'
        api_secret = loads(boto3.client('secretsmanager').get_secret_value(SecretId=api_secret_name)['SecretString'])

        partner_id = 'impel' if ENV == 'prod' else 'test'
        self.crm_api_config = {
            'partner_id': partner_id,
            'x_api_key': loads(api_secret[partner_id])['api_key'],
            'api_domain': 'https://9x8uvbshtk.execute-api.us-east-1.amazonaws.com/prod' if ENV == 'prod' else 'https://2y26660ywf.execute-api.us-east-1.amazonaws.com/test'
        }

    def test_crm_api(self):
        now = datetime.now(tz=timezone.utc)
        res = test_endpoint(
            endpoint=f'{self.crm_api_config["api_domain"]}/leads',
            method='GET',
            headers={
                'partner_id': self.crm_api_config["partner_id"],
                'x_api_key': self.crm_api_config["x_api_key"],
            },
            params={
                'db_creation_date_start': (now - timedelta(days=1)).isoformat(),
                'db_creation_date_end': now.isoformat(),
                'result_count': 10
            })
        assert len(res.json()['leads']) > 0

    def run(self):
        self.test_crm_api()
