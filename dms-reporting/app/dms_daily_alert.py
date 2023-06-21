"""Daily Check Service Repair Order and Vehicle Sale"""
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

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_active_dealer_integrations(cursor):
    """Return all active dealer integrations."""
    cursor.execute("SELECT id FROM stage.dealer_integration_partner WHERE is_active = true")
    
    rows = cursor.fetchall()

    return [row[0] for row in rows]

def get_service_repair_order_data(cursor):
    """Return service repair order data from yesterday."""
    query = """
        SELECT DISTINCT dealer_integration_partner_id
        FROM stage.service_repair_order
        WHERE date_trunc('day', db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '21 day')
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    return [row[0] for row in rows]

def get_vehicle_sales_data(cursor):
    """Return vehicle sale data from yesterday."""
    query = """
        SELECT DISTINCT dealer_integration_partner_id
        FROM stage.vehicle_sale
        WHERE date_trunc('day', db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '21 day')
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    return [row[0] for row in rows]

def get_yesterday_date():
    """Return yesterday's date as a datetime.date object."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    return yesterday

def alert_topic(dealerlist, data_type):
    """Notify Topic of missing S3 files."""
    yesterday = get_yesterday_date()
    message = f'No {data_type} data uploaded {yesterday} for dealers: {dealerlist}'
    SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message
        )
    
def get_missing_dealer_integrations(active_dealers, previous_data_dealers):
    """Return missing list of dealers with missing data."""

    set_active_dealers = set(active_dealers)
    set_previous_data_dealers = set(previous_data_dealers)

    
    return set_active_dealers - set_previous_data_dealers


def get_dealer_information():
    """Return active dealers and dealers with existing data."""
    dealer_data = {}
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        dealer_data['active_dealers'] = get_active_dealer_integrations(cursor)
        dealer_data['dealers_with_sro'] = get_service_repair_order_data(cursor)
        dealer_data['dealers_with_vs'] = get_vehicle_sales_data(cursor)
               
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error occurred:", error)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return dealer_data
    
      
def lambda_handler(event, context):
    """Daily check for missing service repair order or vehicle sale data."""
    dealer_data = get_dealer_information()
    active_dealers = dealer_data['active_dealers'] 
    dealers_with_sro = dealer_data['dealers_with_sro']
    dealers_with_vs = dealer_data['dealers_with_vs']
    print(dealer_data)

    active_dealers_with_missing_sro = get_missing_dealer_integrations(active_dealers, dealers_with_sro)
    active_dealers_with_missing_vs = get_missing_dealer_integrations(active_dealers, dealers_with_vs)
    
    
    if active_dealers_with_missing_sro:
        alert_topic(active_dealers_with_missing_sro, 'service_repair_order')
    if active_dealers_with_missing_vs:
        alert_topic(active_dealers_with_missing_vs, 'vehicle_sale')  
