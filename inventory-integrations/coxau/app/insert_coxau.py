import os
import json
import logging
import urllib.parse
import boto3
import pandas as pd
from rds_instance import RDSInstance
from psycopg2.extras import execute_values
from json import loads
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

ENVIRONMENT = os.environ['ENVIRONMENT']
IS_PROD = ENVIRONMENT == 'prod'

rds_instance = RDSInstance(IS_PROD)


def extract_vehicle_data(json_data):
    vehicle_data = {
        'vin': json_data.get('inv_vehicle|vin', ''),
        'oem_name': json_data.get('inv_vehicle|oem_name', ''),
        'type': json_data.get('inv_vehicle|type', ''),
        'mileage': json_data.get('inv_vehicle|mileage', ''),
        'make': json_data.get('inv_vehicle|make', ''),
        'model': json_data.get('inv_vehicle|model', ''),
        'year': json_data.get('inv_vehicle|year', ''),
        'stock_num': json_data.get('inv_vehicle|stock_num', ''),
        'new_or_used': json_data.get('inv_vehicle|new_or_used', ''),
    }
    return vehicle_data

def extract_inventory_data(json_data):
    inventory_data = {
        'list_price': None if not json_data.get('inv_inventory|list_price', '').strip() else float(json_data['inv_inventory|list_price']),
        'fuel_type': json_data.get('inv_inventory|fuel_type', '').strip() or None,
        'exterior_color': json_data.get('inv_inventory|exterior_color', '').strip() or None,
        'interior_color': json_data.get('inv_inventory|interior_color', '').strip() or None,
        'doors': None if not json_data.get('inv_inventory|doors', '').strip() else int(json_data['inv_inventory|doors']),
        'seats': None if not json_data.get('inv_inventory|seats', '').strip() else int(json_data['inv_inventory|seats']),
        'transmission': json_data.get('inv_inventory|transmission', '').strip() or None,
        'drive_train': json_data.get('inv_inventory|drive_train', '').strip() or None,
        'cylinders': None if not json_data.get('inv_inventory|cylinders', '').strip() else int(json_data['inv_inventory|cylinders']),
        'body_style': json_data.get('inv_inventory|body_style', '').strip() or None,
        'series': json_data.get('inv_inventory|series', '').strip() or None,
        'vin': json_data.get('inv_inventory|vin', '').strip() or None,
        'interior_material': json_data.get('inv_inventory|interior_material', '').strip() or None,
        'trim': json_data.get('inv_inventory|trim', '').strip() or None,
        'factory_certified': json_data.get('inv_inventory|factory_certified', False),
        'region': json_data.get('inv_inventory|region', '').strip() or None,
        'on_lot': json_data.get('inv_inventory|on_lot', True),
        'metadata': json_data.get('inv_inventory|metadata', '').strip() or None,
        'received_datetime': json_data.get('inv_inventory|received_datetime', '').strip() or None,
    }
    return inventory_data

def extract_option_data(option_json):
    option_data = {
        'option_description': option_json.get('inv_option|option_description', ''),
        'is_priority': option_json.get('inv_option|is_priority', False)
    }
    return option_data

def process_and_upload_data(bucket, key):
    try:
        decoded_key = urllib.parse.unquote_plus(key)
        s3_obj = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        json_data_list = json.load(BytesIO(s3_obj["Body"].read()))
        
        #  Check if incoming data is older than existing data
        sample_data = json_data_list[0] if json_data_list else None
        if sample_data:
            incoming_received_datetime = sample_data.get('inv_inventory|received_datetime')
            logger.info(type(incoming_received_datetime))
            logger.info(incoming_received_datetime)
            if incoming_received_datetime and not rds_instance.is_new_data(incoming_received_datetime):
                logger.warning("Incoming data is older than existing data. Processing stopped.")
                return
                
        processed_vehicle_ids = []
        for json_data in json_data_list:
            # Retrieve dealer_integration_partner_id
            provider_dealer_id = json_data.get("inv_dealer_integration_partner|provider_dealer_id")
            if provider_dealer_id is None:
                logger.warning(f"No provider dealer ID found for {decoded_key}, skipping.")
                continue  
            
            dealer_integration_partner_id = rds_instance.find_dealer_integration_partner_id(provider_dealer_id)
            if dealer_integration_partner_id is None:
                logger.warning(f"No dealer integration partner ID found for provider dealer ID: {provider_dealer_id}, skipping.")
                continue 

            # Insert vehicle data
            vehicle_data = extract_vehicle_data(json_data)
            vehicle_data['dealer_integration_partner_id'] = dealer_integration_partner_id
            vehicle_id = rds_instance.insert_vehicle(vehicle_data)
            processed_vehicle_ids.append(vehicle_id)

            # Insert inventory data
            inventory_data = extract_inventory_data(json_data)
            inventory_data['vehicle_id'] = vehicle_id
            inventory_data['dealer_integration_partner_id'] = dealer_integration_partner_id
            inventory_id = rds_instance.insert_inventory_item(inventory_data)

            # Insert options data
            if json_data.get('inv_options|inv_options'):
                option_ids = []
                for option_json in json_data['inv_options|inv_options']:
                    option_data = extract_option_data(option_json)
                    option_id = rds_instance.insert_option(option_data)
                    option_ids.append(option_id)
                rds_instance.link_option_to_inventory(inventory_id, option_ids)
            else:
                identifier = json_data.get('inv_vehicle|vin', json_data.get('inv_vehicle|stock_num', 'Unknown Identifier'))
                logger.info(f"Skipping options for record with identifier {identifier} as they do not exist.")
        
        #  Use dealer_integration_partner_id to update the value of on_lot for vehicles
        dealer_id = dealer_integration_partner_id
        rds_instance.update_dealers_other_vehicles(dealer_id, processed_vehicle_ids)
        logger.info(f"Data processing and upload completed for {decoded_key}")

    except Exception as e:
        logger.error(f"Failed to process and upload data for {key}", exc_info=True)
        raise


def lambda_handler(event, context):
    """
    Lambda function handler to process SQS messages and upload JSON data to RDS.
    """
    try:
        for record in event['Records']:
            message = loads(record["body"])
            logger.info(f"Received message: {message}")
            
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                process_and_upload_data(bucket, key)

    except Exception as e:
        logger.exception("Error in Lambda handler")
        raise