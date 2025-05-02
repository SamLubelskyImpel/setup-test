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
from format_utils import parse_consumers, parse_vehicles, parse_datetime, parse_float, parse_int, parse_op_codes


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
NAMESPACES = {'ns0': 'opentrack.dealertrack.com'}


def map_op_codes_to_ro(ro_details, op_codes_dict):
    """Map labor op code to repair order details."""
    try:
        parsed_op_codes = {}

        for repair_order_no, ro_details_xml in ro_details.items():
            op_codes = []

            root = ET.fromstring(ro_details_xml)

            # Find all labor elements
            labor_elements = root.findall('.//ns0:Labor', NAMESPACES)

            for labor_element in labor_elements:
                line_type = labor_element.find('ns0:LineType', NAMESPACES).text
                if line_type != 'A':
                        continue   # only LineType A refers to op code
                op_code = labor_element.find('ns0:LaborOpCode', NAMESPACES).text
                op_codes.append({
                    "op_code|op_code": op_code,
                    "op_code|op_code_desc": op_codes_dict.get(op_code, '')
                })

            parsed_op_codes[repair_order_no] = op_codes

        return parsed_op_codes
    except Exception as e:
        logger.exception("Unable to map labor op code to ro")
        raise e


def get_labor_cost_and_amount(ro_details):
    consumer_cost = 0
    consumer_amount = 0
    warranty_cost = 0
    warranty_amount = 0
    internal_cost = 0
    internal_amount = 0

    for detail in ro_details:
        labor_list = detail.find('ns0:LaborDetails', NAMESPACES).findall('.//ns0:Labor', NAMESPACES) or []

        for labor in labor_list:
            payment_method = labor.find('ns0:LinePaymentMethod', NAMESPACES).text
            labor_cost = parse_float(labor.find('ns0:LaborCost', NAMESPACES).text) or 0
            labor_amount = parse_float(labor.find('ns0:LaborAmount', NAMESPACES).text) or 0

            if payment_method == "C":       # Customer
                consumer_cost += labor_cost
                consumer_amount += labor_amount
            elif payment_method == "W":     # Warranty
                warranty_cost += labor_cost
                warranty_amount += labor_amount
            elif payment_method == "I":     # Internal
                internal_cost += labor_cost
                internal_amount += labor_amount

    return {
        'consumer_cost': consumer_cost,
        'consumer_amount': consumer_amount,
        'warranty_cost': warranty_cost,
        'warranty_amount': warranty_amount,
        'internal_cost': internal_cost,
        'internal_amount': internal_amount,
        'total_cost': consumer_cost + warranty_cost + internal_cost,
    }


def get_parts_cost_and_amount(ro_details):
    consumer_cost = 0
    consumer_amount = 0
    warranty_cost = 0
    warranty_amount = 0
    internal_cost = 0
    internal_amount = 0

    for detail in ro_details:
        parts_list = detail.find('ns0:Parts', NAMESPACES).findall('.//ns0:Part', NAMESPACES) or []

        for part in parts_list:
            trans_group = part.find('ns0:TransGroup', NAMESPACES).text
            quantity = parse_int(part.find('ns0:Quantity', NAMESPACES).text) or 0
            cost = (parse_float(part.find('ns0:Cost', NAMESPACES).text) or 0) * quantity
            net_price = (parse_float(part.find('ns0:NetPrice', NAMESPACES).text) or 0) * quantity    # how much the dealership is charging by the part

            if trans_group == "O" or trans_group == "S":    # Customer
                consumer_cost += cost
                consumer_amount += net_price
            elif trans_group == "W":                        # Warranty
                warranty_cost += cost
                warranty_amount += net_price
            elif trans_group == "I":                        # Internal
                internal_cost += cost
                internal_amount += net_price

    return {
        'consumer_cost': consumer_cost,
        'consumer_amount': consumer_amount,
        'warranty_cost': warranty_cost,
        'warranty_amount': warranty_amount,
        'internal_cost': internal_cost,
        'internal_amount': internal_amount,
        'total_cost': consumer_cost + warranty_cost + internal_cost,
    }


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
        parsed_op_codes = parse_op_codes(ro_json.get('op_codes'))
        mapped_op_codes = map_op_codes_to_ro(ro_json.get('ro_details'), parsed_op_codes)

        for ro_xml in ro_json.get('root_repair_orders'):
            root = ET.fromstring(ro_xml)

            db_dealer_integration_partner = {"dms_id": dms_id}
            db_service_repair_order = {}
            db_vehicle = parsed_vehicles.get(root.find('ns0:VIN', NAMESPACES).text)
            db_consumer = parsed_consumers.get(root.find('ns0:CustomerKey', NAMESPACES).text)
            db_op_codes = mapped_op_codes.get(root.find('ns0:RepairOrderNumber', NAMESPACES).text)

            details_xml = ro_json.get('ro_details').get(root.find('ns0:RepairOrderNumber', NAMESPACES).text)
            details_root = ET.fromstring(details_xml)

            all_details = details_root.findall('.//ns0:ClosedRepairOrderDetail', NAMESPACES)

            labor_prices = get_labor_cost_and_amount(all_details)
            parts_prices = get_parts_cost_and_amount(all_details)

            # Parse repair order data
            db_service_repair_order = {
                "ro_open_date": parse_datetime(
                    root.find('ns0:OpenDate', NAMESPACES).text, '%Y%m%d', '%Y-%m-%d %H:%M:%S'
                ),
                "ro_close_date": parse_datetime(
                    root.find('ns0:CloseDate', NAMESPACES).text, '%Y%m%d', '%Y-%m-%d %H:%M:%S'
                ),
                "txn_pay_type": root.find('ns0:PaymentMethod', NAMESPACES).text,
                "repair_order_no": root.find('ns0:RepairOrderNumber', NAMESPACES).text,
                "total_amount": parse_float(root.find('ns0:TotalSale', NAMESPACES).text),
                "consumer_total_amount": labor_prices['consumer_amount'] + parts_prices['consumer_amount'],
                "warranty_total_amount": labor_prices['warranty_amount'] + parts_prices['warranty_amount'],
                "internal_total_amount": labor_prices['internal_amount'] + parts_prices['internal_amount'],
                "service_order_cost": labor_prices['total_cost'] + parts_prices['total_cost'],
                "consumer_labor_amount": labor_prices['consumer_amount'],
                "consumer_parts_amount": parts_prices['consumer_amount'],
                "warranty_labor_amount": labor_prices['warranty_amount'],
                "warranty_parts_cost": parts_prices['warranty_amount'],
                "customer_pay_parts_cost": parts_prices['consumer_cost'],
                "internal_parts_cost": parts_prices['internal_cost'],
                "total_internal_labor_amount": labor_prices['internal_amount'],
                "total_internal_parts_amount": parts_prices['internal_amount'],
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
