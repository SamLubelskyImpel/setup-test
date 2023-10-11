"""Activity ETL"""
import logging
from json import dumps
from os import environ

from crm_drivers import crm_mapper
from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import DBSession


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    activity_id = event["activity_id"]

    with DBSession() as session:
        activity = session.query(
            Activity
        ).filter(
            Activity.id == activity_id
        ).first()
        if not activity:
            logger.error(f"Activity not found {activity_id}")
            raise

        activity_type = session.query(
            ActivityType
        ).filter(
            ActivityType.id == activity.activity_type_id
        ).first()

        crm_name = activity.lead.consumer.dealer.integration_partner.impel_integration_partner_name

        activity = activity.as_dict()
        activity_type = activity_type.type

    if crm_name.upper() not in crm_mapper:
        logger.error(f"CRM {crm_name} is not defined")
        raise

    crm = crm_mapper[crm_name.upper()]()
    logger.info(f"CRM {crm.name} identified for activity {activity_id}")

    try:
        crm_response = crm.handle_activity(activity, activity_type)
    except Exception as e:
        logger.error(f"An error occured during the handling of an activity {activity_id} {e}")
        raise

    logger.info("{} responded for activity {} with {}".format(crm.name, activity_id, crm_response))
    return dumps(crm_response)
