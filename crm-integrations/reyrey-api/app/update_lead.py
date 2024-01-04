import json
import logging
import os
from typing import Any, Dict
import xml.etree.ElementTree as ET

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(event)
    try:
        # Extract the XML string from the event body
        xml_data = event['body']

        # Register the namespace
        ns = {'ns': 'http://www.starstandards.org/STAR'}
        ET.register_namespace('', ns['ns'])

        root = ET.fromstring(xml_data)

        if root.tag != '{http://www.starstandards.org/STAR}rey_ImpelCRMPublishLeadDisposition':
            raise ValueError("Invalid XML format")

        application_area = root.find(".//ns:ApplicationArea", namespaces=ns)
        record = root.find(".//ns:Record", namespaces=ns)

        dealer_number = None
        store_number = None
        area_number = None
        if application_area is not None:
            sender = application_area.find(".//ns:Sender", namespaces=ns)
            if sender is not None:
                dealer_number = sender.find(".//ns:DealerNumber", namespaces=ns).text
                store_number = sender.find(".//ns:StoreNumber", namespaces=ns).text
                area_number = sender.find(".//ns:AreaNumber", namespaces=ns).text

        if not dealer_number and not store_number and not area_number:
            raise RuntimeError("Unknown dealer id")

        dealer_id = f"{store_number}_{area_number}_{dealer_number}"
        logger.info(f"Dealer ID: {dealer_id}")

        #TODO: verify dealerId against database

        identifier = record.find(".//ns:Identifier", namespaces=ns)
        logger.info(f"Identifier: {identifier}")

        prospect_id = identifier.find(".//ns:ProspectId", namespaces=ns).text

        logger.info(f"Prospect ID: {prospect_id}")

        #TODO: verify prospectId against database

        event_id = record.find(".//ns:RCIDispositionEventId", namespaces=ns).text
        event_name = record.find(".//ns:RCIDispositionEventName", namespaces=ns).text
        logger.info(f"Event ID: {event_id}")
        logger.info(f"Event Name: {event_name}")

        return {
            'statusCode': 200
        }

    except ET.ParseError:
        # Handle XML parsing errors
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid XML'})
        }
    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
