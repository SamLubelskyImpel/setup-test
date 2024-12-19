"""Retrieve activity by activity_id from the shared CRM layer."""
import logging
import json
from json import dumps
from os import environ
from datetime import datetime
from decimal import Decimal
from typing import Any

from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

from utils import get_restricted_query, apply_dealer_timezone


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class CustomEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects."""

    def default(self, obj: Any) -> Any:
        """Serialize datetime and Decimal objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super(CustomEncoder, self).default(obj)


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve activity."""
    logger.info(f"Event: {event}")

    try:
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]
        activity_id = event["pathParameters"]["activity_id"]

        with DBSession() as session:
            query = (
                session.query(DealerIntegrationPartner.id, Dealer.product_dealer_id, Dealer.metadata_, ActivityType.type, Activity.activity_due_ts, Activity.activity_requested_ts, Activity.notes, Activity.contact_method, Activity.lead_id, Lead.crm_lead_id)
                        .join(Activity.lead)
                        .join(Activity.activity_type)
                        .join(Lead.consumer)
                        .join(Consumer.dealer_integration_partner)
                        .join(DealerIntegrationPartner.dealer)
                        .filter(Activity.id == activity_id))

            db_result = get_restricted_query(query, integration_partner).first()

            if not db_result:
                logger.error(f"Activity not found {activity_id}")
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": f"Activity not found {activity_id}"})
                }

        logger.info(f"Found activity {db_result}")

        (dip_id,
         product_dealer_id,
         dealer_metadata,
         activity_type,
         activity_due_ts,
         activity_requested_ts,
         notes,
         contact_method,
         lead_id, 
         crm_lead_id) = db_result

        if dealer_metadata:
            dealer_timezone = dealer_metadata.get("timezone", "")
        else:
            logger.warning(f"No metadata found for dealer: {dip_id}")
            dealer_timezone = ""

        activity_record = {
            "dealer_id": product_dealer_id,
            "activity_type": activity_type,
            "activity_due_ts": apply_dealer_timezone(activity_due_ts, dealer_timezone, dip_id, include_tz_offset=True) if activity_due_ts else None,
            "activity_requested_ts": apply_dealer_timezone(activity_requested_ts, dealer_timezone, dip_id, include_tz_offset=True),
            "notes": notes,
            "contact_method": contact_method,
            "lead_id": lead_id,
            "crm_lead_id": crm_lead_id
        }

        return {
            "statusCode": 200,
            "body": dumps(activity_record, cls=CustomEncoder)
        }

    except Exception as e:
        logger.exception(f"Error retrieving activity: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
