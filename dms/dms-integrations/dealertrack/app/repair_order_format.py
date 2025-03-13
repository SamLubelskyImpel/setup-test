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
from format_utils import parse_consumers, parse_vehicles, parse_datetime, parse_float, parse_int

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
s3_client = boto3.client("s3")


def parse_op_codes(ro_details):
    """Parse labor op code from repair order details."""
    try:
        parsed_op_codes = {}

        for repair_order_no, ro_details_xml in ro_details.items():
            op_codes = []

            root = ET.fromstring(ro_details_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com'}

            # Find all labor elements
            labor_elements = root.findall('.//ns0:Labor', ns0)

            for labor_element in labor_elements:
                op_code = labor_element.find('ns0:LaborOpCode', ns0).text
                op_code_desc = labor_element.find('ns0:LineType', ns0).text

                op_codes.append({
                    "op_code|op_code": op_code,
                    "op_code|op_code_desc": op_code_desc
                })

            parsed_op_codes[repair_order_no] = op_codes

        return parsed_op_codes
    except Exception as e:
        logger.error("Unable to parse labor op code")
        raise e

def parse_labor(labor_list, ns0):
    consumer_labor = 0.0
    warranty_labor = 0.0
    internal_labor = 0.0

    for labor in labor_list:
        line_payment_method = labor.find('ns0:LinePaymentMethod', ns0).text
        labor_amount = parse_float(labor.find('ns0:LaborAmount', ns0).text) or 0.0

        if line_payment_method == "C":      # Customer
            consumer_labor += labor_amount
        elif line_payment_method == "W":    # Warranty
            warranty_labor += labor_amount
        elif line_payment_method == "I":
            internal_labor += labor_amount

    return (consumer_labor, warranty_labor, internal_labor)

def parse_parts(parts_list, ns0):
    consumer_parts = 0.0
    warranty_parts = 0.0
    internal_parts = 0.0

    for part in parts_list:
        trans_group = part.find('ns0:TransGroup', ns0).text

        quantity = parse_int(part.find('ns0:Quantity', ns0).text) or 0
        cost = parse_float(part.find('ns0:NetPrice', ns0).text) or 0.0

        part_amount = quantity * cost

        if trans_group == "O" or trans_group == "S": # Customer Pay
            consumer_parts += part_amount
        elif trans_group == "W":                     # Warranty
            warranty_parts += part_amount
        elif trans_group == "I":                     # Internal
            internal_parts += part_amount

    return (consumer_parts, warranty_parts, internal_parts)

def get_internal_parts_cost(parts_list, ns0):
    internal_parts = 0.0

    for part in parts_list:
        trans_group = part.find('ns0:TransGroup', ns0).text

        if trans_group == "I": # Customer Pay
            internal_parts += parse_float(part.find('Cost', ns0).text)

    return internal_parts

def parse_xml_to_entries(ro_json, s3_uri):
    """Format dealertrack repair order xml data to unified format."""
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
        parsed_consumers = parse_consumers(ro_json.get('customers'))
        parsed_vehicles = parse_vehicles(ro_json.get('vehicles'))
        parsed_op_codes = parse_op_codes(ro_json.get('ro_details'))

        for ro_xml in ro_json.get('root_repair_orders'):
            root = ET.fromstring(ro_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com'}

            db_dealer_integration_partner = {"dms_id": dms_id}
            db_service_repair_order = {}
            db_vehicle = parsed_vehicles.get(root.find('ns0:VIN', ns0).text)
            db_consumer = parsed_consumers.get(root.find('ns0:CustomerKey', ns0).text)
            db_op_codes = parsed_op_codes.get(root.find('ns0:RepairOrderNumber', ns0).text)

            details_xml = ro_json.get('ro_details').get(root.find('ns0:RepairOrderNumber', ns0).text)
            details_root = ET.fromstring(details_xml)

            all_details = details_root.findall('.//ns0:ClosedRepairOrderDetail', ns0)

            consumer_parts_total = 0.0
            warranty_parts_total = 0.0
            internal_parts_total = 0.0
            internal_parts_total_cost = 0.0

            consumer_labor_total = 0.0
            warranty_labor_total = 0.0
            internal_labor_total = 0.0

            for detail in all_details:
                labor_list = detail.find('ns0:LaborDetails',ns0).findall('.//ns0:Labor', ns0) or []
                parts_list = detail.find('ns0:Parts',ns0).findall('.//ns0:Part', ns0) or []

                consumer_parts, warranty_parts, internal_parts = parse_parts(parts_list, ns0)
                consumer_labor, warranty_labor, internal_labor = parse_labor(labor_list, ns0)

                consumer_parts_total+=consumer_parts
                warranty_parts_total+= warranty_parts
                internal_parts_total+= internal_parts
                consumer_labor_total += consumer_labor
                warranty_labor_total += warranty_labor
                internal_labor_total += internal_labor
                internal_parts_total_cost += get_internal_parts_cost(parts_list, ns0)

            labor_total = consumer_labor_total + warranty_labor_total + internal_labor_total
            parts_total = consumer_parts_total + warranty_parts_total + internal_parts_total

            # Parse repair order data
            db_service_repair_order = {
                "ro_open_date": parse_datetime(
                    root.find('ns0:OpenDate', ns0).text, '%Y%m%d', '%Y-%m-%d %H:%M:%S'
                ),
                "ro_close_date": parse_datetime(
                    root.find('ns0:CloseDate', ns0).text, '%Y%m%d', '%Y-%m-%d %H:%M:%S'
                ),
                "txn_pay_type": root.find('ns0:PaymentMethod', ns0).text,
                "repair_order_no": root.find('ns0:RepairOrderNumber', ns0).text,
                "total_amount": parse_float(root.find('ns0:TotalSale', ns0).text),
                "consumer_total_amount": consumer_labor_total + consumer_parts_total,
                "warranty_total_amount": warranty_labor_total + warranty_parts_total,
                "internal_total_amount": internal_labor_total + internal_parts_total,
                "service_order_cost": (
                    labor_total + parts_total
                ),
                "consumer_labor_amount": consumer_labor_total,
                "consumer_parts_amount": consumer_parts_total,
                "customer_pay_parts_cost": consumer_parts_total,
                "warranty_labor_amount": warranty_labor_total,
                "warranty_parts_cost": warranty_parts_total,
                "total_internal_labor_amount": internal_labor_total,
                "total_internal_parts_amount": internal_parts_total,
                "internal_parts_cost": internal_parts_total_cost
            }

            metadata = dumps(db_metadata)
            db_vehicle["metadata"] = metadata
            db_consumer["metadata"] = metadata
            db_service_repair_order["metadata"] = metadata

            entry = {
                "dealer_integration_partner": db_dealer_integration_partner,
                "service_repair_order": db_service_repair_order,
                "vehicle": db_vehicle,
                "consumer": db_consumer,
                "op_codes.op_codes": db_op_codes,
            }

            entries.append(entry)

        logger.info("Inserting entries: %s", entries)
        return entries, dms_id
    except Exception as e:
        logger.error("Unable to parse service repair orders")
        raise e


def record_handler(record: SQSRecord) -> None:
    """Transform DealerTrack repair order files."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        bucket = body['Records'][0]['s3']['bucket']['name']
        key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(key)
        response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        content = response['Body'].read().decode('utf-8')
        ro_json = loads(content)
        entries, dms_id = parse_xml_to_entries(ro_json, decoded_key)
        upload_unified_json(entries, "repair_order", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming Dealertrack repair order record: {record}")
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
