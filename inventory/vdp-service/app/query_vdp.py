"""Get VDP data based on VIN or Stock No and dealer ID."""

import csv
from io import StringIO
import logging
from os import environ
from json import loads, dumps

import boto3

INVENTORY_BUCKET = environ["INVENTORY_BUCKET"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")


def fetch_vdp_data(impel_dealer_id):
    """Fetch VDP data based on dealer ID."""
    try:
        # Get VDP data from S3
        s3_key = f"vdp/{impel_dealer_id}.csv"
        response = s3_client.get_object(Bucket=INVENTORY_BUCKET, Key=s3_key)

        # Read and process the CSV file
        csv_content = response['Body'].read().decode('utf-8')
        return csv.DictReader(StringIO(csv_content))
    except Exception as e:
        logger.exception(f"Error retrieving VDP data from inventory_integration bucket: {e}")
        raise FileNotFoundError(f"VDP data file not found for dealer ID {impel_dealer_id}.")


def lambda_handler(event, context):
    """Get VDP data based on VIN or Stock No and dealer ID."""
    logger.info(f"Event: {event}")
    try:
        query_params = event.get('queryStringParameters') or {}
        vin = query_params.get('vin')
        stock_no = query_params.get('stock_number')
        impel_dealer_id = query_params.get('impel_dealer_id')

        # Validate VIN or Stock No and Dealer ID
        if not (vin or stock_no) or not impel_dealer_id:
            logger.error("VIN or Stock No and Dealer ID are required.")
            return {
                "statusCode": 400,
                "body": dumps({"error": "VIN or Stock No and Dealer ID are required"})
            }

        # Get VDP data from S3
        vdp_reader = fetch_vdp_data(impel_dealer_id)

        # Find the matching record based on VIN or Stock No
        matching_record = next(
            (record for record in vdp_reader if (vin and record.get('VIN') == vin) or (stock_no and record.get('STOCK') == stock_no)),
            None
        )

        if matching_record:
            logger.info(f"Matching record found: {matching_record}")
        else:
            raise ValueError(f"VDP data not found for VIN {vin} or Stock No {stock_no}.")

        return {
            "statusCode": 200,
            "body": dumps(matching_record)
        }
    except (FileNotFoundError, ValueError) as e:
        logger.warning(f"Record not found: {e}")
        return {
            "statusCode": 404,
            "body": dumps({"error": str(e)})
        }
    except Exception as e:
        logger.exception(f"Error retrieving VDP data: {e}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
