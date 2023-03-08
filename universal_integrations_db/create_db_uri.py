# imports will be needed for use with secrets manager
import boto3
from json import loads

SM_CLIENT = boto3.client('secretsmanager')

def __get_db_secrets(secretId):
    """ Get DB secrets from SecretsManager """
    SecretString = loads(SM_CLIENT.get_secret_value(
        SecretId=secretId
    )['SecretString'])

    #dialect+driver://username:password@host:port/database
    return 'postgresql://{}:{}@{}/{}'.format(
        SecretString['user'], SecretString['password'], SecretString['host'], SecretString['db_name']
    )

def create_db_uri(env):
    """ Construct and return database URI """
    if env == 'prod':
        pass
    else:
       uri = __get_db_secrets('universal_integration/target_db')

    return uri