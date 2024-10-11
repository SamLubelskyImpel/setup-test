import json
from os import environ

from datetime import datetime, timedelta
from dateutil.tz import tzutc
ENVIRONMENT = environ.get("ENVIRONMENT")



def is_token_expired(secrets_response):
    created_date = secrets_response['CreatedDate']
    secret_string = json.loads(secrets_response['SecretString'])
    expires_in = int(secret_string['expires_in'])

    expiration_date = created_date + timedelta(seconds=expires_in)
    current_time = datetime.now(tzutc())

    is_expired = current_time > expiration_date

    return is_expired

