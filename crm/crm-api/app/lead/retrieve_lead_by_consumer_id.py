"""Retrieve lead by crm_lead_id from the shared CRM layer."""
import logging
import json
from json import dumps
from os import environ
from datetime import datetime
from decimal import Decimal
from typing import Any
from sqlalchemy import desc

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.session_config import DBSession

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
    """Retrieve lead by crm_lead_id. crm_dealer_id and integration_partner_name."""
    return
