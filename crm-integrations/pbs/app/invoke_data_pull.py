"""Invoke PBS data pull."""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from api_wrappers import PbsApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def build_id_list(type: str, leads: list):
    ids = []
    for lead in leads:
        if type == "Contact":
            buyer_ref = lead.get("BuyerRef", "")
            if buyer_ref:
                ids.append(buyer_ref)
        elif type == "Vehicle":
            vehicles = lead.get("Vehicles", [])
            if vehicles:
                vehicle_ref = vehicles[0].get("VehicleRef", "")
                if vehicle_ref:
                    ids.append(vehicle_ref)
    return ",".join(ids)


def find_value(type: str, dataset: list, id: str):
    for item in dataset:
        if type == "Contact":
            if item.get("ContactId", "") == id:
                return item
        if type == "Vehicle":
            if item.get("VehicleId", "") == id:
                return item
    logger.warning(
        f"Could not find {type} with ID {id}. Lead may not transform correctly"
    )
    return {}


def fetch_new_leads(sort_time_start, sort_time_end, crm_dealer_id: str):
    """Fetch new leads from PBS CRM."""
    api = PbsApiWrapper()

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3)

    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S") + ".0000000Z"
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S") + ".0000000Z"

    try:
        inital_leads = api.call_deal_get(
            start_time_str, end_time_str, crm_dealer_id
        ).get("Deals", [])

    except Exception as e:
        logger.error(f"Error occured calling PBS APIs: {e}")
        raise

    logger.info(f"Total initial leads found {len(inital_leads)}")

    # Filter leads
    filtered_leads = filter_leads(inital_leads)
    logger.info(f"Total leads after filtering {len(filtered_leads)}")

    sorted_leads = sort_leads_by_creation_date(
        filtered_leads, sort_time_start, sort_time_end
    )
    logger.info(f"Total leads after sorting {len(sorted_leads)}")

    contact_id_list = build_id_list("Contact", sorted_leads)
    vehicle_id_list = build_id_list("Vehicle", sorted_leads)

    if contact_id_list:
        contact_list = api.call_contact_get(contact_id_list, crm_dealer_id).get(
            "Contacts", []
        )
    if vehicle_id_list:
        vehicle_list = api.call_vehicle_get(vehicle_id_list, crm_dealer_id).get(
            "Vehicles", []
        )

    for lead in sorted_leads:
        contact_id = lead.get("BuyerRef", None)
        vehicle_id = None
        deal_id = lead.get("DealId")

        vehicles = lead.get("Vehicles", [])
        if len(vehicles) > 0:
            vehicle_id = vehicles[0].get("VehicleRef", None)

        if contact_id:
            try:
                lead["Contact_Info"] = find_value("Contact", contact_list, contact_id)
            except Exception as e:
                logger.warning(
                    f"Error getting contact info: {e}, skipping lead {deal_id}"
                )
                continue

        if vehicle_id:
            try:
                lead["Vehicle_Info"] = find_value("Vehicle", vehicle_list, vehicle_id)
            except Exception as e:
                logger.warning(
                    f"Error getting vehicle info: {e}, skipping lead {deal_id}"
                )
                continue

    return sorted_leads


def sort_leads_by_creation_date(leads: list, start_time: str, end_time: str):
    """Sort leads by CreationDate and filter them by the provided time range."""
    sorted_leads = sorted(leads, key=lambda x: x.get("CreationDate", ""), reverse=True)

    filtered_leads = []
    for lead in sorted_leads:
        creation_date_str = lead.get("CreationDate")
        if not creation_date_str:
            continue

        print(f"This is creation_date_str: {creation_date_str}")
        print(f"{start_time} <= {creation_date_str} <= {end_time}")

        if start_time <= creation_date_str <= end_time:
            filtered_leads.append(lead)

    return filtered_leads


def filter_leads(leads: list):
    """Filter leads by LeadType."""
    filtered_leads = []
    for lead in leads:
        try:
            lead_type = lead.get("LeadType", "")

            if lead_type == "Internet":
                filtered_leads.append(lead)

        except Exception as e:
            logger.error(
                f"Error parsing Lead type for lead {lead.get('DealId')}. Skipping lead: {e}"
            )
            continue

    return filtered_leads


def save_raw_leads(leads: list, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/pbs/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving leads to {s3_key}")
    s3_client.put_object(
        Body=dumps(leads),
        Bucket=BUCKET,
        Key=s3_key,
    )


def convert_timestamp(input_timestamp: str) -> str:
    """Convert a timestamp from 'YYYY-MM-DDTHH:MM:SSZ' format to 'YYYY-MM-DDTHH:MM:SS.0000000Z' format."""
    dt = datetime.strptime(input_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    dt_with_ms = dt.strftime("%Y-%m-%dT%H:%M:%S") + ".0000000Z"

    return dt_with_ms


def record_handler(record: SQSRecord):
    """Invoke PBS data pull."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        logger.info(body)

        start_time = body.get("start_time")
        end_time = body.get("end_time")
        crm_dealer_id = body.get("crm_dealer_id")
        product_dealer_id = body.get("product_dealer_id", "missing_product_dealer")

        leads = fetch_new_leads(
            convert_timestamp(start_time), convert_timestamp(end_time), crm_dealer_id
        )
        if not leads:
            logger.info(
                f"No new leads found for dealer with serial number {crm_dealer_id} from {start_time} to {end_time}"
            )
            return

        save_raw_leads(leads, product_dealer_id)
        logger.info(f"Total leads saved {len(leads)}")

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        logger.error(
            "[SUPPORT ALERT] Failed to Get Leads [CONTENT] ProductDealerId: {}\nDealerId: {}\nStartTime: {}\nEndTime: {}\n\nTraceback: {}".format(
                product_dealer_id, crm_dealer_id, start_time, end_time, e
            )
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
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
    return