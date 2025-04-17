import logging
from os import environ
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from rds_instance import RDSInstance
import boto3
from json import dumps, loads
from uuid import uuid4
from tekion_wrapper import TekionWrapper
from requests.exceptions import HTTPError
from datetime import datetime, timezone
from typing import Any, Dict


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

MISSING_DATA_STATUS_CODES = [400, 404]

s3_client = boto3.client('s3')

def save_vehicle_sales(data, dms_id):
    now = datetime.now(tz=timezone.utc)
    key = f"tekion-apc/raw/fi_closed_deal/{dms_id}/{now.year}/{now.month}/{now.day}/{now.isoformat()}_fi_closed_deal_{str(uuid4())}.json"
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=key,
        Body=dumps(data)
    )
    logger.info(f"Saved raw response to {key}")


def enrich_vehicle_sale(data, dms_id, api):

    deal_id = data["id"]

    enriched_record = {}

    enriched_record["deal"] = data

    try:
        enriched_record.update({"api_payment": api.get_deal_payment(deal_id)})
    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No payment details for deal {deal_id}")
            enriched_record.update({"api_payment": {}})
        else:
            raise e

    try:
        enriched_record.update({"api_service_contracts": api.get_deal_service_contracts(deal_id)})
    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No service contracts for deal {deal_id}")
            enriched_record.update({"api_service_contracts": []})
        else:
            raise e
  
    try:
        enriched_record.update({"api_trade_ins": api.get_deal_trade_ins(deal_id)})
    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No trade ins for deal {deal_id}")
            enriched_record.update({"api_trade_ins": []})
        else:
            raise e

    # try:
    #     enriched_record.update({"api_gross_details": api.get_deal_gross_details(deal_id)})
    # except HTTPError as e:
    #     if e.response.status_code in MISSING_DATA_STATUS_CODES:
    #         logger.warning(f"No gross details for deal {deal_id}")
    #         enriched_record.update({"api_gross_details": {}})
    #     else:
    #         raise e

    vehicles = []
    try:
        vehicles = api.get_deal_vehicles(deal_id)

        enriched_record.update({"api_vehicles": vehicles})

    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No vehicles for deal {deal_id}")
            enriched_record.update({"api_vehicles": []})
        else:
            raise e

    warranties = []
    for vehicle in vehicles:
        try:
            warranties.append(api.get_vehicle_warranties(vehicle["id"]))
        except HTTPError as e:
            if e.response.status_code in MISSING_DATA_STATUS_CODES:
                logger.warning(f"No warranties for vehicle {vehicle['id']}")
            else:
                raise e

    enriched_record.update({"api_warranties": warranties})

    try:
        customers = api.get_deal_customers(deal_id)

        buyerFlag, cobuyerFlag = False, False

        for customer in customers:

            comms = {}
            
            if not buyerFlag and customer["type"] == "BUYER":
                customer_details = api.get_customer_v4(customer["id"])
                if customer_details:
                    comms = customer_details[0].get("customerDetails", {})
                enriched_record.update({"api_buyer_communications": comms})
                enriched_record.update({"api_buyer": customer})
                buyerFlag = True
            elif not cobuyerFlag and customer["type"] == "COBUYER":
                customer_details = api.get_customer_v4(customer["id"])
                if customer_details:
                    comms = customer_details[0].get("customerDetails", {})
                enriched_record.update({"api_cobuyer_communications": comms})
                enriched_record.update({"api_cobuyer": customer})
                cobuyerFlag = True
            elif buyerFlag and cobuyerFlag:
                break

    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No customers for deal {deal_id}")
            enriched_record.update({"api_buyer": {}})
            enriched_record.update({"api_cobuyer": {}})
        else:
            raise e

    try:
        assignees = api.get_deal_assignees(deal_id)
        for assignee in assignees:
            if assignee["primary"]:
                enriched_record.update({"api_salesperson": api.get_employee(assignee["employeeDetails"]["id"])})
                break
    except HTTPError as e:
        if e.response.status_code in MISSING_DATA_STATUS_CODES:
            logger.warning(f"No salespersons for deal {deal_id}")
            enriched_record.update({"api_salesperson": {}})
        else:
            raise e

    logger.info(f"Enriched record: {enriched_record}")

    return enriched_record


def fetch_and_process_vehicle_sales(body: Dict[str, Any]):
    try:
        dms_id = body["dms_id"]
        s3_key = body["s3_key"]

        api = TekionWrapper(dealer_id=dms_id)

        vehicle_sale_chunk= loads(s3_client.get_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key)["Body"].read().decode("utf-8"))

        enriched_records = []

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(enrich_vehicle_sale, vehicle_sale, dms_id, api)
                for vehicle_sale in vehicle_sale_chunk
            ]

            try:
                for future in as_completed(futures):
                    enriched_vehicle_sale = future.result()
                    enriched_records.append(enriched_vehicle_sale)
            except Exception as e:
                logger.error(f"Unhandled exception for dealer {dms_id}: {e}")
                raise e

        save_vehicle_sales(enriched_records, dms_id)

    except HTTPError as e:
        if e.response.status_code == 429:
            logger.error("Tekion API returned 429")
            return
        logger.exception("HTTP error")
        raise
    except Exception as e:
        logger.exception("Error processing record")
        raise

def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    fetch_and_process_vehicle_sales(record.json_body)

def process_vehicle_sales_handler(event, context):
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
    except:
        logger.exception(f"Error processing batch")
        raise