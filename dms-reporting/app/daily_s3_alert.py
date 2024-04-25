"""Daily check for s3 files from integration partners"""
import boto3
import logging
from datetime import date, datetime, timezone, timedelta
from json import dumps, loads
from os import environ
import psycopg2
from utils.db_config import get_connection

env = environ["ENVIRONMENT"]

SNS_CLIENT = boto3.client('sns')
SNS_TOPIC_ARN = environ["CEAlertTopicArn"]

schema = 'prod' if env == 'prod' else 'stage'

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_integration_partners():
    """Return all integration partners."""
    partners = []
    
    conn = get_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT impel_integration_partner_id FROM {schema}.integration_partner")
        rows = cursor.fetchall()
        
        for partner in rows:
            partner = partner[0]
            if partner.lower() not in ['cdk', 'dealertrack', 'dealervault']:
                partners.append(partner)

        conn.commit()

    except (Exception, psycopg2.Error) as error:
        # Handle any errors that occur during database operations
        logger.info("Error occurred:", error)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return partners

def check_folder_has_files(bucket, prefix):
    """Check the existence of files in an S3 Folder."""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        if obj['Key'] != prefix:  # ignore the folder itself
            return True
    return False


def get_yesterday_date():
    """Return yesterday's date as a datetime.date object."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    return yesterday
    
def alert_topic(partner, file_type, yesterday):
    """Notify Topic of missing S3 files."""
    
    message = f'{partner}: No {file_type} files uploaded for {yesterday}'
    SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message
        )

    return 'ok'

def lambda_handler(event, context):
    """Check integration partner for previous day data upload."""

    partners = get_integration_partners()
    bucket_name = "integrations-us-east-1-prod" if env == 'prod' else 'integrations-us-east-1-test'
    yesterday = get_yesterday_date()

    for partner in partners:
        # sidekick doesn't have 'fi_closed_deal' files
        file_types = ['repair_order'] if partner == 'sidekick' else ['fi_closed_deal', 'repair_order']
        if partner in ['sidekick', 'tekion']:
            continue  # sidekick and tekion are not live yet, so just skip them for now
        for file_type in file_types:
            file_key = f'{partner}/{file_type}/{yesterday.year}/{yesterday.month}/{yesterday.day}/'   
            if not check_folder_has_files(bucket_name, file_key):
                alert_topic(partner, file_type, yesterday)
        
