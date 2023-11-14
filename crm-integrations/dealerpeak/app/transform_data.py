"""Transform raw dealerpeak data to the unified format"""

import boto3
import logging
from os import environ
from json import loads
from typing import Any


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


s3_client = boto3.client('s3')


# def record_handler() -> Any:
#     """Process individual SQS record."""
#     logger.info(f"Record: {record}")

#     try:

#         # Process json_data as needed

#         for item in json_data:
#             salesperson = json_data.get('agent', None)
#             salesperson_id = salesperson.get('userID', None)
#             salesperson_first_name = salesperson.get('givenName', None)
#             salesperson_last_name = salesperson.get('familyName', None)
#             salesperson_emails = salesperson.get('contactInformation', None).get('emails', None)
#             logger.info(salesperson_id, salesperson_first_name, salesperson_last_name, salesperson_emails)  

#     except Exception as e:
#         logger.error(f"Error processing record: {e}")
#         raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealerpeak data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        for record in event["Records"]:
            message = loads(record["body"])
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read()
                json_data = loads(content)
                logger.info(f"Data: {json_data}")
    except Exception:
        logger.exception(f"Error transforming dealerpeak file {event}")
        raise
