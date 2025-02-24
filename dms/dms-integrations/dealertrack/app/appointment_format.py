"""Format Dealertrack xml data to unified format."""
import logging
import urllib.parse
import xml.etree.ElementTree as ET
from json import dumps, loads
from os import environ
from typing import Any

import boto3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

from unified_df import upload_unified_json
from format_utils import parse_consumers, parse_vehicles, parse_datetime

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
s3_client = boto3.client("s3")


def parse_xml_to_entries(appointment_json, s3_uri):
    """Format dealertrack service appointment xml data to unified format."""
    try:
        entries = []

        # s3_uri structure: dealertrack-dms/raw/{resource}/{dealer_id}/{year}/{month}/{day}/{timestamp}_{resource}.json
        dms_id = s3_uri.split('/')[3]

        db_metadata = {
            "Region": REGION,
            "PartitionYear": s3_uri.split("/")[4],
            "PartitionMonth": s3_uri.split("/")[5],
            "PartitionDate": s3_uri.split("/")[6],
            "s3_url": s3_uri,
        }

        # Parse customers
        parsed_consumers = parse_consumers(appointment_json.get('customers'))
        parsed_vehicles = parse_vehicles(appointment_json.get('vehicles'))

        for appointment_xml in appointment_json.get('root_appointments'):
            root = ET.fromstring(appointment_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com/transitional'}

            db_dealer_integration_partner = {"dms_id": dms_id}
            db_appointment = {}
            db_vehicle = parsed_vehicles.get(root.find('ns0:VIN', ns0).text)
            db_consumer = parsed_consumers.get(root.find('ns0:CustomerKey', ns0).text)
            db_service_contracts = []
            db_op_codes = []

            # Parse appointment data
            appointment_datetime = root.find('ns0:AppointmentDateTime', ns0).text
            db_appointment["appointment_time"] = parse_datetime(
                appointment_datetime, '%Y%m%d%H%M', '%H:%M:%S'
            )
            db_appointment["appointment_date"] = parse_datetime(
                appointment_datetime, '%Y%m%d%H%M', '%Y-%m-%d'
            )

            open_transaction_date = root.find('ns0:OpenTransactionDate', ns0).text
            db_appointment["appointment_create_ts"] = parse_datetime(
                open_transaction_date, '%Y%m%d', '%Y-%m-%d %H:%M:%S'
            )

            db_appointment["appointment_no"] = root.find('ns0:AppointmentNumber', ns0).text

            # Parse Op Codes
            details_element = root.find('ns0:Details', ns0)
            if details_element is not None:
                for appointment_detail in details_element.findall('ns0:AppointmentDetail', ns0):
                    labor_op_code = appointment_detail.find('ns0:LaborOpCode', ns0).text
                    line_type = appointment_detail.find('ns0:LineType', ns0).text
                    db_op_codes.append({
                        "op_code|op_code": labor_op_code,
                        "op_code|op_code_desc": line_type
                    })

            metadata = dumps(db_metadata)
            db_vehicle["metadata"] = metadata
            db_consumer["metadata"] = metadata
            db_appointment["metadata"] = metadata

            entry = {
                "dealer_integration_partner": db_dealer_integration_partner,
                "appointment": db_appointment,
                "vehicle": db_vehicle,
                "consumer": db_consumer,
                "service_contracts.service_contracts": db_service_contracts,
                "op_codes.op_codes": db_op_codes
            }
            entries.append(entry)
        logger.info("Inserting entries: %s", entries)
        return entries, dms_id
    except Exception as e:
        logger.error("Unable to parse appointment")
        raise e


def record_handler(record: SQSRecord) -> None:
    """Transform DealerTrack appointment record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        bucket = body['Records'][0]['s3']['bucket']['name']
        key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(key)
        response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        content = response['Body'].read().decode('utf-8')
        appointment_json = loads(content)
        entries, dms_id = parse_xml_to_entries(appointment_json, decoded_key)
        upload_unified_json(entries, "service_appointment", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming Dealertrack appointment record {record}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealertrack data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
