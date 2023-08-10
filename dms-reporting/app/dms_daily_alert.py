"""Daily Check Service Repair Order and Vehicle Sale"""
import boto3
import logging
from datetime import date, timedelta
from os import environ
import psycopg2
from utils.db_config import get_connection


env = environ["ENVIRONMENT"]

SNS_CLIENT = boto3.client('sns')
SNS_TOPIC_ARN = environ["DMS_REPORTING_SNS_TOPIC"]
schema = 'prod' if env == 'prod' else 'stage'

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_active_dealer_integrations(cursor):
    """Return all active dealer integrations."""
    cursor.execute(f"SELECT id FROM {schema}.dealer_integration_partner WHERE is_active = true")
    
    rows = cursor.fetchall()

    return [row[0] for row in rows]

def get_service_repair_order_data(cursor):
    """Return service repair order data from yesterday."""
    query = f"""
        SELECT DISTINCT dealer_integration_partner_id
        FROM {schema}.service_repair_order
        WHERE date_trunc('day', db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '1 day')
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    return [row[0] for row in rows]

def get_vehicle_sales_data(cursor):
    """Return vehicle sale data from yesterday."""
    query = f"""
        SELECT DISTINCT dealer_integration_partner_id
        FROM {schema}.vehicle_sale
        WHERE date_trunc('day', db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '1 day')
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    return [row[0] for row in rows]


def get_impel_dealer_ids_by_integration_partner_ids(cursor, ids):
    """Return Impel dealer IDs based on a set of integration partner IDs sorted by integration partner."""
    query = f"""
        SELECT d.impel_dealer_id, ip.impel_integration_partner_id
        FROM {schema}.dealer_integration_partner dip
        JOIN {schema}.dealer d ON dip.dealer_id = d.id
        JOIN {schema}.integration_partner ip ON dip.integration_partner_id = ip.id
        WHERE dip.id IN %s
    """
    cursor.execute(query, (tuple(ids),))
    rows = cursor.fetchall()

    sorted_ids = {}
    for impel_dealer_id, integration_partner_id in rows:
        if integration_partner_id not in sorted_ids:
            sorted_ids[integration_partner_id] = []
        sorted_ids[integration_partner_id].append(impel_dealer_id)

    return sorted_ids


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
    
def get_missing_dealer_integrations(cursor, active_dealers, previous_data_dealers):
    """Return missing list of dealers with missing data."""

    set_active_dealers = set(active_dealers)
    set_previous_data_dealers = set(previous_data_dealers)
    
    return get_impel_dealer_ids_by_integration_partner_ids(cursor, set_active_dealers - set_previous_data_dealers)


def get_dealer_information():
    """Return active dealers and dealers with existing data."""
    dealer_data = {}
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        active_dealers = get_active_dealer_integrations(cursor)
        dealers_with_sro = get_service_repair_order_data(cursor)
        dealers_with_vs = get_vehicle_sales_data(cursor)
        dealer_data['active_dealers_with_missing_sro'] = get_missing_dealer_integrations(cursor, active_dealers, dealers_with_sro)
        dealer_data['active_dealers_with_missing_vs'] = get_missing_dealer_integrations(cursor, active_dealers, dealers_with_vs)

        conn.commit()

    except (Exception, psycopg2.Error) as error:
        logger.exception("Error occurred: " + str(error))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return dealer_data
    
      
def lambda_handler(event, context):
    """Daily check for missing service repair order or vehicle sale data."""
    dealer_data = get_dealer_information()

    active_dealers_with_missing_sro = dealer_data['active_dealers_with_missing_sro']
    active_dealers_with_missing_vs = dealer_data['active_dealers_with_missing_vs']
    
    if active_dealers_with_missing_sro:
        alert_topic(active_dealers_with_missing_sro, 'service_repair_order')
    if active_dealers_with_missing_vs:
        alert_topic(active_dealers_with_missing_vs, 'vehicle_sale')
