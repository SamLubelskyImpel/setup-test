import logging
import boto3
import json 
import io
import os
from os import environ
import pandas as pd
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from rds_instance import RDSInstance

ENVIRONMENT = environ.get('ENVIRONMENT')
TOPIC_ARN = os.environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())
s3_client = boto3.client('s3')

def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="UpdateVDP Lambda Error",
        Message=str(error_message),
    )
    return

def read_csv_from_s3(s3_body, file_name):
    try:
        return pd.read_csv(io.BytesIO(s3_body), delimiter=',', encoding='utf-8', on_bad_lines='warn', dtype=None, header=0)
    except Exception as e:
        error_message = f"Error processing file: {file_name} - {str(e)}"
        logger.error(error_message)
        raise

def validate_vdp_data(vdp_df, provider_dealer_id, impel_integration_partner_id):
    try:
        # check if file is empty
        if vdp_df.empty:
            logger.warning(f"Warning: {provider_dealer_id}.csv is empty. Skipping further processing..")
            return
        
        # check for required columns: VIN, STOCK and VDP URL
        required_columns = ['VIN', 'STOCK', 'VDP URL']
        missing_columns = [col for col in required_columns if col not in vdp_df.columns]
        if missing_columns:
            logger.warning(f"Warning: Missing columns in the {provider_dealer_id}.csv : {', '.join(missing_columns)}")
            return

        vdp_df = vdp_df.loc[:, required_columns]

        # drop rows with misisng VIN, STOCK or VDP URL
        for col in required_columns:
            if col in vdp_df.columns and vdp_df[col].isnull().any():
                vdp_df.dropna(subset=[col], inplace=True)

        # form a list of tuples for bulk insert using execute_values() 
        vdp_list = [tuple(row) + (impel_integration_partner_id,) for row in vdp_df.to_numpy()]
        
        return vdp_list if vdp_list else None
    except Exception as e:
        error_message = f"Error processing data: {provider_dealer_id}.csv - {str(e)}"
        logger.error(error_message)
        raise

def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")
    
    try:
        sns_message = json.loads(record['body'])
        s3_event = sns_message['Records'][0]['s3']

        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']
        file_name = object_key.split('/')[-1]

        vdp_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        vdp_df = read_csv_from_s3(vdp_obj["Body"].read(), file_name)

        provider_dealer_id = file_name.split('.')[0]

        rds_instance = RDSInstance()
        impel_integration_partner_id = rds_instance.find_dealer_integration_partner_id(provider_dealer_id)

        if impel_integration_partner_id is None:
            logger.warning(f"No integration partner found for dealer id: {provider_dealer_id}. Skipping further processing..")
            return

        logger.info(f"impel_integration_partner_id: {impel_integration_partner_id}")

        vdp_list = validate_vdp_data(vdp_df, provider_dealer_id, impel_integration_partner_id)

        if vdp_list is None:
            logger.warning(f"No VDP data to update: {provider_dealer_id}.csv.")
            return

        logger.info(f"Batch updating {len(vdp_list)} vdp data")
        results = rds_instance.batch_update_inventory_vdp(vdp_list, provider_dealer_id)
        logger.info(f"Updated [{len(results)}] database records")
    except Exception as e:
        message = f"Update VDP Service Failed: {e}"
        logger.exception(message)
        notify_client_engineering(message)
        raise

def lambda_handler(event, context):
    """Download file from S3 and update inventory VDP data"""
    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception:
        logger.exception("Error occurred while processing the event.")
        raise
