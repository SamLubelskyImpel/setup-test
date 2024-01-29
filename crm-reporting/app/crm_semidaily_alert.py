"""Semi-Daily Check Leads, Lead Updates, and Activity data."""
import boto3
import logging
from datetime import datetime, timedelta
from os import environ
import psycopg2
from utils.db_config import get_connection


ENVIRONMENT = environ.get("ENVIRONMENT")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

SNS_CLIENT = boto3.client("sns")
schema = "prod" if ENVIRONMENT == "prod" else "test"

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_active_dealer_integrations(cursor):
    """Return all active dealer integrations."""
    cursor.execute(f"SELECT id FROM {schema}.crm_dealer_integration_partner WHERE is_active = true")
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_new_lead_data(cursor):
    """Return new lead data from the last 12 hours."""
    query = f"""
        SELECT DISTINCT dealer_integration_partner_id
        FROM {schema}.crm_lead
        JOIN {schema}.crm_consumer on crm_consumer.id = crm_lead.consumer_id
        WHERE date_trunc('day', crm_lead.db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '12 hours')
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_lead_update_data(cursor):
    """Return lead update data from the last 12 hours."""
    query = f"""
        SELECT DISTINCT dealer_integration_partner_id
        FROM {schema}.crm_lead
        JOIN {schema}.crm_consumer on crm_consumer.id = crm_lead.consumer_id
        WHERE date_trunc('day', crm_lead.db_update_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '12 hours')
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_activity_data(cursor):
    """Return activity data from the last 12 hours."""
    query = f"""
        SELECT DISTINCT dealer_integration_partner_id
        FROM {schema}.crm_activity
        JOIN {schema}.crm_lead on crm_lead.id = crm_activity.lead_id
        JOIN {schema}.crm_consumer on crm_consumer.id = crm_lead.consumer_id
        WHERE date_trunc('day', crm_activity.db_creation_date) = date_trunc('day', CURRENT_TIMESTAMP - INTERVAL '12 hours')
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_impel_dealer_ids_by_integration_partner_ids(cursor, ids):
    """Return Product dealer IDs based on a set of integration partner IDs sorted by integration partner."""
    query = f"""
        SELECT d.product_dealer_id, ip.impel_integration_partner_name
        FROM {schema}.crm_dealer_integration_partner dip
        JOIN {schema}.crm_dealer d ON dip.dealer_id = d.id
        JOIN {schema}.crm_integration_partner ip ON dip.integration_partner_id = ip.id
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


def get_previous_date():
    """Return the datetime 12 hours ago as a datetime.datetime object."""
    current_date = datetime.now()
    previous_date = current_date - timedelta(hours=12)
    return previous_date.strftime("%m/%d/%Y %H:%M:%S")


def alert_topic(dealerlist, data_type):
    """Notify Topic of missing shared layer data."""
    report_date = get_previous_date()
    message = f"No new {data_type} data uploaded to shared layer for 12 hours after {report_date} for dealers: {dealerlist}"
    SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="CRM Shared Layer Alerts: Missing Data for 12 Hours",
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
        dealers_with_leads = get_new_lead_data(cursor)
        dealers_with_updates = get_lead_update_data(cursor)
        dealers_with_activities = get_activity_data(cursor)

        dealer_data["active_dealers_with_missing_leads"] = get_missing_dealer_integrations(cursor, active_dealers, dealers_with_leads)
        dealer_data["active_dealers_with_missing_updates"] = get_missing_dealer_integrations(cursor, active_dealers, dealers_with_updates)
        dealer_data["active_dealers_with_missing_activities"] = get_missing_dealer_integrations(cursor, active_dealers, dealers_with_activities)

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
    """Semi-Daily check for missing leads, lead updates, or activity data."""
    dealer_data = get_dealer_information()

    active_dealers_with_missing_leads = dealer_data["active_dealers_with_missing_leads"]
    active_dealers_with_missing_updates = dealer_data["active_dealers_with_missing_updates"]
    active_dealers_with_missing_activities = dealer_data["active_dealers_with_missing_activities"]

    if active_dealers_with_missing_leads:
        alert_topic(active_dealers_with_missing_leads, "lead")

    if active_dealers_with_missing_updates:
        alert_topic(active_dealers_with_missing_updates, "lead update")

    if active_dealers_with_missing_activities:
        alert_topic(active_dealers_with_missing_activities, "activity")
