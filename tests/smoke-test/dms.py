from utils import test_endpoint, BaseSmokeTest
from consts import ENV
from json import loads
import boto3


class DMSSmokeTest(BaseSmokeTest):
    def __init__(self):
        super().__init__()

        api_secret_name = f'{ENV}/DmsDataService'
        api_secret = loads(boto3.client('secretsmanager').get_secret_value(SecretId=api_secret_name)['SecretString'])

        self.api_config = {
            'client_id': 'impel_service',
            'x_api_key': loads(api_secret['impel_service'])['api_key'],
            'api_domain': 'https://w8gcusqqd5.execute-api.us-east-1.amazonaws.com/prod' if ENV == 'prod' else 'https://r4buanomya.execute-api.us-east-1.amazonaws.com/test'
        }

    def test_repair_order_inbound(self):
        res = test_endpoint(
            endpoint=f'{self.api_config["api_domain"]}/repair-order/v1',
            method='GET',
            headers={
                'client_id': self.api_config['client_id'],
                'x_api_key': self.api_config['x_api_key']
            },
            params={
                'impel_dealer_id': 'john_kennedy_mazda',
                'page': '1',
                'result_count': '10',
                'ro_close_date_end': '2024-08-12',
                'ro_close_date_start': '2024-07-13'
            } if ENV == 'prod' else {
                'result_count': '10'
            })
        assert len(res.json()['results']) > 0

    def run(self):
        self.test_repair_order_inbound()