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


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

s3_client = boto3.client('s3')

def save_vehicle_sale(data, dms_id):
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

    data.update("payment", api.get_deal_payment(deal_id))

    data.update("service_contracts", api.get_deal_service_contracts(deal_id))

    data.update("trade_ins", api.get_deal_trade_ins(deal_id))

    data.update("gross_details", api.get_deal_gross_details(deal_id))

    vehicles = api.get_deal_vehicles(deal_id)

    data.update("vehicles", vehicles)

    warranties = []
    for vehicle in vehicles:
        warranties.append(api.get_vehicle_warranties(vehicle["id"]))
    
    data.update("warranties", warranties)

    customers = api.get_deal_customers(deal_id)

    buyerFlag, cobuyerFlag = False, False

    for customer in customers:

        if not buyerFlag and customer["type"] == "BUYER":
            data.update("buyer", api.get_customer_v4(customer["id"]))
            buyerFlag = True
        elif not cobuyerFlag and customer["type"] == "COBUYER":
            data.update("cobuyer", api.get_customer_v4(customer["id"]))
            cobuyerFlag = True
        elif buyerFlag and cobuyerFlag:
            break
    
    assignees = api.get_deal_assignees(deal_id)

    for assignee in assignees:
        if assignee["primary"]:
            deal.update("salesperson", api.get_employee(assignee["id"]))
            break

    return data


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        body = record.json_body
        dms_id = body.get("dms_id")
        s3_key = body.get("s3_key")

        api = TekionWrapper(dealer_id=dms_id)

        vehicle_sale_chunk= loads(s3_client.get_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key))

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(enrich_vehicle_sale, vehicle_sale, dms_id, api)
                for vehicle_sale in vehicle_sale_chunk
            ]

            for future in as_completed(futures):
                try:
                    enriched_vehicle_sale = future.result()
                    save_vehicle_sale(enriched_vehicle_sale, dms_id)
                except Exception as e:
                    logger.error(f"Unhandled exception for dealer {dealer_id}: {e}")
                    dealer_codes[dealer_id] = "ERROR"  

    except HTTPError as e:
        if e.response.status_code == 429:
            logger.error("Tekion API returned 429")
            return
        logger.exception("HTTP error")
        raise
    except Exception as e:
        logger.exception("Error processing record")
        raise


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