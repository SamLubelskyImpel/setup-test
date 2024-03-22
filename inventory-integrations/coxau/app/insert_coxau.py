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


# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS services
s3_client = boto3.client('s3')

# Environment variables
ENVIRONMENT = os.environ['ENVIRONMENT']
IS_PROD = ENVIRONMENT == 'prod'

# RDSInstance initialization
rds_instance = RDSInstance(IS_PROD)


def extract_vehicle_data(json_data):
    vehicle_data = {
        'vin': json_data['inv_vehicle|vin'],
        'oem_name': json_data['inv_vehicle|oem_name'],
        'type': json_data['inv_vehicle|type'],
        'mileage': json_data['inv_vehicle|mileage'],
        'make': json_data['inv_vehicle|make'],
        'model': json_data['inv_vehicle|model'],
        'year': json_data['inv_vehicle|year'],
        'vehicle_class': json_data['inv_vehicle|vehicle_class'],
        'stock_num': json_data['inv_vehicle|stock_num'],
        'new_or_used': json_data['inv_vehicle|new_or_used'],
    }
    return vehicle_data

def extract_inventory_data(json_data):
    inventory_data = {
        'list_price': json_data['inv_inventory|list_price'],
        'fuel_type': json_data['inv_inventory|fuel_type'],
        'exterior_color': json_data['inv_inventory|exterior_color'],
        'interior_color': json_data['inv_inventory|interior_color'],
        'doors': json_data['inv_inventory|doors'],
        'seats': json_data['inv_inventory|seats'],
        'transmission': json_data['inv_inventory|transmission'],
        'drive_train': json_data['inv_inventory|drive_train'],
        'cylinders': json_data['inv_inventory|cylinders'],
        'body_style': json_data['inv_inventory|body_style'],
        'series': json_data['inv_inventory|series'],
        'vin': json_data['inv_inventory|vin'],
        'interior_material': json_data['inv_inventory|interior_material'],
        'trim': json_data['inv_inventory|trim'],
        'factory_certified': json_data['inv_inventory|factory_certified'],
        'region': json_data['inv_inventory|region'],
        'on_lot': json_data['inv_inventory|on_lot'],
    }
    return inventory_data

def extract_option_data(option_json):
    option_data = {
        'option_description': option_json.get('inv_option|option_description', ''),
        'is_priority': option_json.get('inv_option|is_priority', False)
    }
    return option_data

def extract_equipment_data(equipment_json):
    equipment_data = {
        'equipment_description': equipment_json.get('inv_equipment|equipment_description', ''),
        'is_optional': equipment_json.get('inv_equipment|is_optional', False)
    }
    return equipment_data

def process_and_upload_data(bucket, key):
    try:
        decoded_key = urllib.parse.unquote_plus(key)
        s3_obj = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        # json_data = json.load(BytesIO(s3_obj["Body"].read()))
        json_data_list = json.load(BytesIO(s3_obj["Body"].read()))
        
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

            # Insert inventory data
            inventory_data = extract_inventory_data(json_data)
            inventory_data['vehicle_id'] = vehicle_id
            inventory_data['dealer_integration_partner_id'] = dealer_integration_partner_id
            inventory_id = rds_instance.insert_inventory_item(inventory_data)

            # Insert equipment data and link to inventory
            equipment_ids = []
            for equipment_json in json_data['inv_equipments|inv_equipments']:
                equipment_data = extract_equipment_data(equipment_json)
                equipment_id = rds_instance.insert_equipment(equipment_data)
                equipment_ids.append(equipment_id)
            rds_instance.link_equipment_to_inventory(inventory_id, equipment_ids)

            # Insert options data and link to inventory
            option_ids = []
            for option_json in json_data['inv_options|inv_options']:
                option_data = extract_option_data(option_json)
                option_id = rds_instance.insert_option(option_data)
                option_ids.append(option_id)
            rds_instance.link_option_to_inventory(inventory_id, option_ids)

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