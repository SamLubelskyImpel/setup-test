import logging
import os
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
import  boto3
from json import loads
import urllib.parse


IS_PROD = int(os.environ.get('IS_PROD'))
SALESAI_ARN = os.environ.get('SALESAI_ARN')
SERVICEAI_ARN = os.environ.get('SERVICEAI_ARN')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

s3 = boto3.client('s3')
sts = boto3.client('sts')
secrets = boto3.client('secretsmanager')


def get_product_configs(product: str) -> dict:
    secret_name = f'{"prod" if IS_PROD else "test"}/CDPI/SyndicationConfig'
    response = loads(secrets.get_secret_value(SecretId=secret_name)['SecretString'])
    configs = loads(response[product.upper()])
    configs['ROLE_ARN'] = SALESAI_ARN if product == 'salesai' else SERVICEAI_ARN
    return configs


def upload_file(configs: dict, filename: str, source_path: str, product: str):
    bucket, role, path = configs['BUCKET'], configs['ROLE_ARN'], configs["PATH"]
    response = sts.assume_role(
        RoleArn=role,
        RoleSessionName="AssumeRoleSession"
    )
    # get the temporary credentials
    credentials = response['Credentials']
    # use the temporary credentials to put the object
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    key = f'{path}/{filename}'
    s3_client.upload_file(source_path, bucket, key)
    logger.info(f'[{product}] Scores syndicated to {bucket} {key}')


def record_handler(record: SQSRecord):
    logger.info(f'Record: {record}')

    event = record.json_body
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key: str = event['Records'][0]['s3']['object']['key']
    decoded_key = urllib.parse.unquote(file_key)
    split_key = decoded_key.split('/')
    product, filename = split_key[1], split_key[-1]

    try:
        # Download the S3 file to /tmp (Lambda's temp storage)
        download_path = f'/tmp/{os.path.basename(decoded_key)}'
        s3.download_file(bucket_name, decoded_key, download_path)
        product_configs = get_product_configs(product)
        upload_file(product_configs, filename, download_path, product)
    except:
        logger.exception(f'[{product}] Failed to syndicate scores to product')
        raise


def lambda_handler(event, context):
    """Lambda function entry point for processing SQS messages."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except:
        logger.exception(f"Error processing records")
        raise
