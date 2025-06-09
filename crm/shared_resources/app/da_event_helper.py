
import logging
import requests
from os import environ
from datetime import datetime
from utils import get_secrets

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def create_webhook_payload(lead_information, consumer_information):
    """Create webhook payload specification."""

    vehicle_of_interest = lead_information.get('vehicles_of_interest')
    if vehicle_of_interest and isinstance(vehicle_of_interest, list):
        vehicle_of_interest = vehicle_of_interest[0]
    else:
        vehicle_of_interest = {}

    pulsar_timestamp = datetime.strptime(lead_information['db_creation_date'], "%Y-%m-%dT%H:%M:%S.%f%z")
    metadata = lead_information.get('metadata')
    metadata = {} if metadata is None else metadata
    vehicle_metadata = vehicle_of_interest.get('metadata')
    vehicle_metadata = {} if vehicle_metadata is None else vehicle_metadata
    return {
        'chatbot_platform_user': consumer_information['dealer_id'],
        'created_utc': lead_information['lead_ts'],
        'pulsar_timestamp': datetime.strftime(pulsar_timestamp, '%Y-%m-%dT%H:%M:%S'),
        'lead_status': lead_information.get('lead_status', ''),
        'lead_status_type': lead_information.get('lead_substatus', ''),
        'lead_type': lead_information.get('lead_origin', ''),
        'email': consumer_information.get('email', ''),
        'first_name': consumer_information.get('first_name', ''),
        'last_name': consumer_information.get('last_name', ''),
        'middle_name': consumer_information.get('middle_name', ''),
        'phone': consumer_information.get('phone', ''),
        'vin': vehicle_of_interest.get('vin', ''),
        'comments': lead_information.get('lead_comment', ''),
        'prospect_source': lead_information.get('lead_source', ''),
        'lead_info': {
            'dealer_id': consumer_information['dealer_id'],
            'lead_id': lead_information['lead_id'],
            'from': 'UNIFIED_CRM_LAYER',
            'created_utc': vehicle_of_interest.get('db_creation_date', ''),
            "phones": [
                {
                    "phone_type": "Cell",
                    "phone_number": consumer_information.get('phone', ''),
                    "is_primary": True,
                    "do_not_call": False,
                    "do_not_text": consumer_information.get('sms_optin_flag', False)
                }
            ],
            "emails": [
                {
                    "email_address": consumer_information.get('email', ''),
                    "is_primary": True,
                    "do_not_email": consumer_information.get('email_optin_flag', False)
                }
            ],
            'metadata': metadata
        },
        'vehicle_info': {
            'Year': vehicle_of_interest.get('year', ''),
            'Make': vehicle_of_interest.get('make', ''),
            'Model': vehicle_of_interest.get('model', ''),
            'Trim': vehicle_of_interest.get('trim', ''),
            'Stock': vehicle_of_interest.get('stock', ''),
            'Bodystyle': vehicle_of_interest.get('body_style', ''),
            'Interiorcolor': vehicle_of_interest.get('interior_color', ''),
            'ExteriorColor': vehicle_of_interest.get('exterior_color', ''),
            'Price': vehicle_of_interest.get('price', ''),
            'Metadata': {k: v for k, v in vehicle_metadata.items() if k in ['min_price', 'max_price']}
                    },
        'appraisalLink': metadata.get('appraisalLink', '')
    }


def send_to_webhook(payload):
    """Send payload to DA webhook."""
    webhook_data = get_secrets("crm-integrations-partner", "DA_EVENT_WEBHOOK")

    logger.info(f"Sending payload to webhook: {webhook_data['url']}")

    webhook_response = requests.post(
        url=f"{webhook_data['url']}",
        headers={"Authorization": f"Bearer {webhook_data['token']}"},
        json=payload
    )

    logger.info(f"Webhook response: {webhook_response.text}")
    return webhook_response.status_code
