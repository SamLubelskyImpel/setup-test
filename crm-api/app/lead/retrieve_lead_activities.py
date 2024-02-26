"""Retieve lead activites."""

import logging
from os import environ
from datetime import datetime
from json import dumps, JSONEncoder
from typing import Any

from crm_orm.models.activity import Activity
from crm_orm.models.lead import Lead
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class CustomEncoder(JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj: Any) -> Any:
        """Serialize datetime and Decimal objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(CustomEncoder, self).default(obj)


def lambda_handler(event: Any, context: Any) -> Any:
    """Update activity."""
    logger.info(f"Event: {event}")

    try:
        lead_id = event["pathParameters"]["lead_id"]

        with DBSession() as session:
            # Check lead existence
            lead = session.query(Lead).filter(Lead.id == lead_id).first()
            if not lead:
                logger.error(f"Lead {lead_id} not found. Activity failed to be updated.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead {lead_id} not found."})
                }

            db_activities = session.query(Activity).filter(
                Activity.lead_id == lead_id
            ).order_by(
                Activity.db_creation_date.desc()
            ).all()

            activities = []
            for activity in db_activities:
                activities.append({
                    "activity_id": activity.id,
                    "activity_type": activity.activity_type.type,
                    "activity_due_ts": activity.activity_due_ts,
                    "activity_requested_ts": activity.activity_requested_ts,
                    "notes": activity.notes,
                    "contact_method": activity.contact_method,
                    "crm_activity_id": activity.crm_activity_id,
                    "db_creation_date": activity.db_creation_date
                })

            if not activities:
                logger.error(f"No activities found for lead {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No activities found for lead {lead_id}"})
                }

            logger.info(f"Founds activities for lead {lead_id}: {activities}")

        return {
            "statusCode": "200",
            "body": dumps(activities, cls=CustomEncoder)
        }

    except Exception as e:
        logger.error(f"Error retrieving lead activities: {str(e)}")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
