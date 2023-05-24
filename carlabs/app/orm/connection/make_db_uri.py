from typing import Literal
from json import loads
import boto3
import os


IS_PROD = os.environ.get('ENVIRONMENT') == 'prod'
SM_CLIENT = boto3.client('secretsmanager')


def __get_db_secret(secret_id):
    return loads(SM_CLIENT.get_secret_value(SecretId=secret_id)['SecretString'])


dms_secret = __get_db_secret(f'{"prod" if IS_PROD else "test"}/DMSDB')
carlabs_di_secret = __get_db_secret(f'{"prod" if IS_PROD else "test"}/carlabs/data_integrations')


DB = {
    'CARLABS_DATA_INTEGRATIONS': {
        'DB_USERNAME': carlabs_di_secret['DB_USERNAME'],
        'DB_PASSWORD': carlabs_di_secret['DB_PASSWORD'],
        'DB_HOST': carlabs_di_secret['DB_HOST'],
        'DB_PORT': carlabs_di_secret['DB_PORT'],
        'DB_NAME': carlabs_di_secret['DB_NAME']
    },
    'SHARED_DMS': {
        'DB_USERNAME': dms_secret['user'],
        'DB_PASSWORD': dms_secret['password'],
        'DB_HOST': dms_secret['host'],
        'DB_PORT': dms_secret['port'],
        'DB_NAME': dms_secret['db_name']
    }
}


def make_db_uri(db: Literal['CARLABS_DATA_INTEGRATIONS', 'SHARED_DMS'], region=None):
    app_name = 'carlabs-etl'
    return 'postgresql://{}:{}@{}:{}/{}?application_name={}'.format(
        DB[db]['DB_USERNAME'],
        DB[db]['DB_PASSWORD'],
        DB[db]['DB_HOST'],
        DB[db]['DB_PORT'],
        DB[db]['DB_NAME'],
        app_name)
