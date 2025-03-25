import logging
import boto3
import json 
import io
import os
from os import environ
import pandas as pd
import re
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

def validate_vdp_data(vdp_df, provider_dealer_id, dealer_integration_partner_id):
    try:
        # check if file is empty
        if vdp_df.empty:
            logger.warning(f"Warning: {provider_dealer_id}.csv is empty. Skipping further processing..")
            return None, None
        
        vin_col = stock_col = vdp_url_col = None
        extracted_cols = []

        # identify relevant columns and handle scenarios where the CSV might have either VIN or stock
        # or varying header names for these fields.
        for col in vdp_df.columns:
            if re.search(r'vin', col, re.IGNORECASE):
                vin_col = col
                extracted_cols.append(col)
            elif re.search(r'stock', col, re.IGNORECASE):
                stock_col = col
                extracted_cols.append(col)
            elif re.search(r'vdp.*url', col, re.IGNORECASE):
                vdp_url_col = col
                extracted_cols.append(col)

        # check for required fields: VIN and/or STOCK and VDP URL
        if vin_col is None and stock_col is None:
            message = f"Missing vin and stock in the {provider_dealer_id}.csv. Skipping further processing.."
            logger.warning(message)
            return None, None
        if vdp_url_col is None:
            message = f"No VDP data in the {provider_dealer_id}.csv. Skipping further processing.."
            logger.warning(message)
            return None, None

        vdp_df = vdp_df.loc[:, extracted_cols]

        # drop rows with misisng VDP URL
        vdp_df = vdp_df.dropna(subset=[vdp_url_col])

        # form a list of tuples for bulk insert using execute_values() 
        vdp_list = [tuple(row) + (dealer_integration_partner_id,) for row in vdp_df.to_numpy()]

        vdp_column_list = [col for col, cond in zip(['vin', 'stock', 'vdp_url'], [vin_col, stock_col, vdp_url_col]) if cond]
        vdp_column_list.append("dealer_integration_partner_id")
        
        return (vdp_list, vdp_column_list) if vdp_list else (None, None)
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
        dealer_integration_partner_id = rds_instance.find_dealer_integration_partner_id(provider_dealer_id)

        if dealer_integration_partner_id is None:
            logger.warning(f"No dealer integration partner id found for dealer id: {provider_dealer_id}. Skipping further processing..")
            return

        logger.info(f"DealerIntegrationPartner: {dealer_integration_partner_id}")

        vdp_list, vdp_column_list = validate_vdp_data(vdp_df, provider_dealer_id, dealer_integration_partner_id)

        if vdp_list is None:
            logger.warning(f"No VDP data to update: {provider_dealer_id}.csv.")
            return

        # construct JOIN conditions and column list for UPDATE query
        # Note: vin and stock_num are columns from table inv_vehicle AS v
        join_conditions = [f"t.vin = v.vin" if 'vin' in vdp_column_list  else "",
                           f"t.stock = v.stock_num" if 'stock' in vdp_column_list else ""]
        join_conditions = " OR ".join(filter(None, join_conditions))
        vdp_column_list = ",".join(vdp_column_list)

        logger.info(f"Batch updating {len(vdp_list)} vdp data")
        results = rds_instance.batch_update_inventory_vdp(vdp_list, vdp_column_list, join_conditions, provider_dealer_id)
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
