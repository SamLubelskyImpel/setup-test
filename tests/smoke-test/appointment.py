from consts import ENV
from utils import test_endpoint, BaseSmokeTest
from json import loads
import boto3


class AppointmentSmokeTest(BaseSmokeTest):
    def __init__(self):
        super().__init__()

        api_secret_name = f'{ENV}/appointment-service'
        api_secret = loads(boto3.client('secretsmanager').get_secret_value(SecretId=api_secret_name)['SecretString'])

        partner_id = 'impel_test' if ENV == 'prod' else 'test'
        self.api_config = {
            'partner_id': partner_id,
            'x_api_key': loads(api_secret[partner_id])['api_key'],
            'api_domain': 'https://2fy7jtilh6.execute-api.us-east-1.amazonaws.com/prod' if ENV == 'prod' else 'https://appointment-service-test.testenv.impel.io',
            'dealer_integration_partner_id': '1',
            'op_code': '12382910' if ENV == 'prod' else 'PRODUCT001',
        }

    def test_xtime_retrieve_appointments(self):
        res = test_endpoint(
            f'{self.api_config["api_domain"]}/appointments/v1',
            method='GET',
            headers={
                'partner_id': self.api_config['partner_id'],
                'x_api_key': self.api_config['x_api_key']
            },
            params={
                'dealer_integration_partner_id': self.api_config['dealer_integration_partner_id'],
                'vin': '5YFHPRAE2LP046808' if ENV == 'prod' else '1HGBH41JXMN109186'
            })
        assert len(res.json()['appointments']) > 0

    def run(self):
        self.test_xtime_retrieve_appointments()
