import logging
import boto3
from json import dumps, loads
from os import environ
from typing import Any
from datetime import datetime
from uuid import uuid4

from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")

BUCKET = environ.get("INTEGRATIONS_BUCKET")

class MissingSellerDataError(Exception):
    pass

def is_dealer_active(vehicle: dict):

    seller = vehicle.get("Seller", None)
    if seller:
        identifier = seller.get("Identifier", None)
        if identifier:
            rds_instance = RDSInstance()
           
            # active_dealers = rds_instance.select_db_active_dealer_partners("carsales")
            # active_dealers = [dealer[0] for dealer in active_dealers]
            # logger.info(f"Active dealers: {active_dealers}")

            results = rds_instance.get_active_dealer_with_id(identifier)
            logger.info(f"Found Impel Dealer Id: {results}")
            impel_dealer_id = None

            if results:
                impel_dealer_id = results[0][0]
            else:
                logger.warning("Impel Dealer Id not found.")
                raise Exception(f"Internal Error: No Impel Dealer Id found for CarSales Dealer {identifier}")

            if identifier not in active_dealers:
                return False, impel_dealer_id

            return True, impel_dealer_id
        else:
            logger.warning("No Seller Identifier provided.")
            raise MissingSellerDataError("Bad Request: Vehicle missing Seller Identifier")
    else:
        logger.warning("No Seller information provided")
        raise MissingSellerDataError("Bad Request: Vehicle missing Seller Data")    


def save_raw_vehicle(vehicle: str, dealer_id: str):
    """Save raw vehicles to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"landing-zone/carsales/{dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving CarSales vehicle to {s3_key}")
    s3_client.put_object(
        Body=dumps(vehicle),
        Bucket=BUCKET,
        Key=s3_key
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """This API handler takes the data sent by CarSales and puts the raw JSON into the S3 bucket."""
    try:
        logger.info(f"Event: {event}")

        body = loads(event["body"])

        logger.info(f"Vehicle body: {body}")

        is_active, dealer_id = is_dealer_active(body)
        if is_active:
            save_raw_vehicle(dumps(body), dealer_id)
            return {"statusCode": 200}
        else:
            vehicle_id = body.get("Identifier", None)
            notify_client_engineering("[SUPPORT ALERT]: CarSales Vehicle Data Received for Inactive Dealer", f"Dealer for vehicle {vehicle_id} with dealer id f{dealer_id} is not active")
            return {"statusCode": 400}
    except MissingSellerDataError as e:
        logger.error(f"Error getting CarSales create vehicle: {str(e)}")
        notify_client_engineering("[SUPPORT ALERT]: Error Occured During CarSales Inventory Ingestion", e)
        return {
            "statusCode": 400,
            "body": dumps({"error": "Bad Request."}),
        }            
    except Exception as e:
        logger.error(f"Error getting CarSales create vehicle: {str(e)}")
        notify_client_engineering("[SUPPORT ALERT]: Error Occured During CarSales Inventory Ingestion", e)
        return {
            "statusCode": 500,
            "body": dumps({"error": "Internal Server Error. Please contact Impel support."}),
        }

def notify_client_engineering(subject, error_message):

    sns_client = boto3.client("sns")
    topic_arn = environ.get(SNS_TOPIC_ARN)
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=str(error_message),
    )
    return