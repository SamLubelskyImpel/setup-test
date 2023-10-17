"""Create activity."""

import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    request_product = event["headers"]["partner_id"]
    lead_id = event["pathParameters"]["lead_id"]

    activity_type = body["activity_type"].lower()
    activity_due_ts = body.get("activity_due_ts")
    activity_requested_ts = body["activity_requested_ts"]
    notes = body.get("notes", "")

    with DBSession() as session:
        # Check lead existance
        lead = session.query(
            Lead
        ).filter(
            Lead.id == lead_id
        ).first()
        if not lead:
            logger.error(f"Lead {lead_id} not found. Activity failed to be created.")
            return {
                "statusCode": "404"
            }

        activity_type_db = session.query(
            ActivityType
        ).filter(
            ActivityType.type == activity_type
        ).first()
        if not activity_type_db:
            logger.error(f"Failed to find activity type {activity_type} for lead {lead_id}")
            raise

        # Create activity
        activity = Activity(
            lead_id=lead.id,
            activity_type_id=activity_type_db.id,
            activity_requested_ts=activity_requested_ts,
            request_product=request_product,
            notes=notes
        )
        if activity_due_ts:
            activity.activity_due_ts = activity_due_ts

        session.add(activity)
        session.commit()

        activity_id = activity.id

    logger.info(f"Created activity {activity_id}")

    # Start ETL?

    return {
        "statusCode": "201",
        "body": dumps({"activity_id": activity_id})
    }
