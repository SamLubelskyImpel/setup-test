"""Activity ETL"""
import logging
from json import dumps
from os import environ

from crm_drivers import crm_mapper

from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.session_config import DBSession


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    body = event["body"]
    integration_partner_id = body["integration_partner_id"]
    activity_id = body["activity_id"]

    with DBSession as session:
        crm_name = session.query(
            IntegrationPartner.impel_integration_partner_name
        ).filter(
            IntegrationPartner.id == integration_partner_id
        ).first()

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

    return dumps(response)
