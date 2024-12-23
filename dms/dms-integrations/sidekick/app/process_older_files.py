from repair_orders import parse_data
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda function to process older daily and historical files for a dealer.
    """

    start_date_str = event.get("start_date")
    end_date_str = event.get("end_date")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S")

    sqs_message_data_mock = {
        "dealer_id": event.get("dealer_id"),
    }
    
    logger.info(f"Processing files for dealer {sqs_message_data_mock['dealer_id']} from {start_date_str} to {end_date_str}")

    while start_date <= end_date:
        
        logger.info(f"Processing files - date {start_date.strftime('%Y-%m-%dT%H:%M:%S')}")

        sqs_message_data_mock["end_dt_str"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        parse_data(sqs_message_data_mock)

        start_date += timedelta(days=1)
    
    logger.info(f"Finished processing files for dealer {sqs_message_data_mock['dealer_id']}")