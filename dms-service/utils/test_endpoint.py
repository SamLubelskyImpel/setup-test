import requests
import boto3
from os import environ
from json import loads

AWS_PROFILE = environ["AWS_PROFILE"]

if AWS_PROFILE == "unified-prod":
    url = "https://dms-service.impel.io/"
else:
    url = "https://dms-service.testenv.impel.io/"

client_id = "impel_service"


secret = boto3.client("secretsmanager").get_secret_value(
    SecretId=f"{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}/DmsDataService"
)
secret = loads(secret["SecretString"])[client_id]
x_api_key = loads(secret)["api_key"]

endpoint = "vehicle-sale"

headers = {
    'accept': 'application/json',
    'client_id': client_id,
    'x_api_key': x_api_key,
}

response = requests.get(f'{url}{endpoint}/v1', headers=headers)

data = response.json()

print(data)
