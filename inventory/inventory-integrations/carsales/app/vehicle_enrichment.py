"""This module is responsible for enriching the vehicle data with RedBook and VDP data."""

import json
import logging
import uuid
from datetime import datetime
from os import environ

import boto3
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from api_wrapper import VDPApiWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")

ENVIRONMENT = environ.get("ENVIRONMENT")


def merge_data(vehicle_data, redbook_data):
    """This function merges the vehicle data with the RedBook data."""
    merged_data = {**vehicle_data, "options": [*redbook_data["results"]]}
    return merged_data


def parse_response(raw):
    """This function parses the raw response from the RedBook Lambda function."""
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw.strip('"')

    return json.loads(raw)


def get_redbook_payload(vehicle_data):
    """This function creates the payload for the RedBook Lambda function."""
    logger.info(vehicle_data["Specification"])
    specification_source = vehicle_data["Specification"]["SpecificationSource"]
    if specification_source != "REDBOOK":
        raise Exception(
            f"Invalid SpecificationSource: Expected 'REDBOOK', but found {specification_source}"
        )

    redbook_code = vehicle_data["Specification"]["SpecificationCode"]
    payload = {"redbookCode": redbook_code}

    logger.info(f"Successfully created payload with Redbook code: {payload}")
    return payload


def get_redbook_data(vehicle_data):
    """This function calls the RedBook Lambda function to get the RedBook data."""
    function_name = f"redbook-{ENVIRONMENT}-RedBookDataFunction"
    payload = get_redbook_payload(vehicle_data)

    try:
        redbook_response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        raw_payload = redbook_response["Payload"].read().decode("utf-8")
        logger.info(f"Redbook Lambda Response: {raw_payload}")
    except Exception as e:
        logger.exception(f"Failed to get Redbook Data {redbook_response}: {e}")
        raise

    try:
        redbook_data = json.loads(json.loads(raw_payload))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode Redbook data: {type(raw_payload)} {e}")
        raise Exception(f"Invalid JSON response from Redbook Lambda: {e}")

    if "results" not in redbook_data or not redbook_data["results"]:
        logger.warning("Redbook data contains no results.")
        raise Exception("No results found in Redbook data.")

    return redbook_data


def record_handler(record: SQSRecord):
    """This function calls and merges the RedBook and VDP data with the vehicle data."""
    logger.info(f"Record: {record}")
    try:
        # Extract the bucket name and object key from the SNS message
        sns_message = json.loads(record["body"])
        s3_event = sns_message["Records"][0]["s3"]

        bucket_name = s3_event["bucket"]["name"]
        object_key = s3_event["object"]["key"]
        impel_dealer_id = str(object_key).split("/")[2]

        # Fetch the object from the S3 bucket
        logger.info(f"Fetching object: {object_key} from bucket: {bucket_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read().decode("utf-8")

        logger.info(f"File Content: {file_content}")
        vehicle_data = json.loads(file_content)
        logger.info(f"Vehicle data: {vehicle_data}")

        # Call the RedBook Lambda function to get the RedBook data
        logger.info(f"Invoking RedBook Integration Lambda for object: {object_key}")
        redbook_data = get_redbook_data(vehicle_data)

        # Merge the vehicle data with the RedBook data
        merged_data = merge_data(vehicle_data, redbook_data)
        logger.info(f"Enriched data: {merged_data}")

        # Call the VDP Lambda function to get the VDP data
        vdp_api = VDPApiWrapper()
        vdp_data = vdp_api.get_vdp_data(vehicle_data, impel_dealer_id)
        logger.info(f"VDP data: {vdp_data}")

        # Merge the enriched data with the VDP data
        if vdp_data:
            merged_data["VDP"] = vdp_data.get("VDP URL")
            merged_data["PHOTO_URL"] = vdp_data.get("SRP IMAGE URL")
            logger.info(f"Enriched data with VDP: {merged_data}")
        else:
            logger.warning("No VDP data found skipping merge.")

        # Save the enriched data to the S3 bucket
        current_time = datetime.now()
        unique_id = str(uuid.uuid4())
        iso_timestamp = current_time.isoformat()

        year = current_time.strftime("%Y")
        month = current_time.strftime("%m")
        day = current_time.strftime("%d")

        merged_object_key = f"raw/carsales/{impel_dealer_id}/{year}/{month}/{day}/{iso_timestamp}_{unique_id}.json"
        logger.info(f"Saving enriched data to: {bucket_name}/{merged_object_key}")
        s3_client.put_object(
            Bucket=bucket_name, Key=merged_object_key, Body=json.dumps(merged_data)
        )
    except Exception:
        logger.exception("Failed to process event")
        raise


def lambda_handler(event, context):
    """This function is the entry point for the Lambda function."""
    logger.info(f"Event: {event}")
    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
        return result
    except Exception:
        logger.exception("Error occurred while processing the event.")
        raise
