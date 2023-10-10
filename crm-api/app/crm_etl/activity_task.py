"""Activity ETL"""
import logging
from json import dumps, loads
from os import environ

from crm_drivers import crm_mapper
from crm_orm.models.activity import Activity
from crm_orm.session_config import DBSession


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    activity_id = body["activity_id"]

    with DBSession() as session:
        activity = session.query(
            Activity
        ).filter(
            Activity.id == activity_id
        ).first()
        if not activity:
            logger.error(f"Activity not found {activity_id}")
            raise

        crm_name = activity.lead.dealer.integration_partner.impel_integration_partner_name
        activity_type = activity.type

    if crm_name.upper() not in crm_mapper:
        logger.error(f"CRM {crm_name} is not defined")
        raise

    crm = crm_mapper[crm_name.upper()]()
    logger.info(f"CRM {crm.name} identified for activity {activity_id}")

    try:
        response = crm.handle_activity(activity_id)
    except Exception as e:
        logger.error(f"An error occured during the handling of an activity {activity_id} {e}")
        raise

    logger.info(f"{crm.name} integration status code {response.status_code} for {activity_type}")
    return dumps(response)
