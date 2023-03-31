"""Return appropriate DB URI."""
import boto3
from json import loads

SM_CLIENT = boto3.client('secretsmanager')


def __get_db_secrets(secretId):
    """Get DB secrets from SecretsManager."""
    SecretString = loads(SM_CLIENT.get_secret_value(
        SecretId=secretId
    )['SecretString'])

    return 'postgresql://{}:{}@{}/{}'.format(
        SecretString['user'], SecretString['password'], SecretString['host'], SecretString['db_name']
    )


def create_db_uri(env):
    """Construct and return database URI."""
    sm_env = f"{env}/DMSDB"
    uri = __get_db_secrets(sm_env)        

    return uri
