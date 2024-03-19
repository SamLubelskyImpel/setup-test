import boto3
from json import loads
from os import environ
from typing import Any
import logging
import pandas as pd
from datetime import datetime
import tempfile
from ftplib import FTP
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from rds_instance import RDSInstance

ENVIRONMENT = environ["ENVIRONMENT"]
INVENTORY_BUCKET = environ["INVENTORY_BUCKET"]
MERCH_FTP_KEY = environ["MERCH_FTP_KEY"]
AI_FTP_KEY = environ["AI_FTP_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sm_client = boto3.client("secretsmanager")


def proccess_and_upload_to_ftp(icc_formatted_inventory, csv_file_path, product_dealer_id, secret_key) -> None:
    """Upload to ftp server."""
    # Upload to FTP
    hostname, username, password = get_ftp_secrets("inventory-integrations-ftp", secret_key)
    prefix = '' if ENVIRONMENT == 'prod' else 'deleteme_'
    filename = f"{prefix}{product_dealer_id}.csv"
    with FTP(hostname) as ftp:
        ftp.login(username, password)

        with open(csv_file_path, 'rb') as file:
            ftp.storbinary(f'STOR {filename}', file)

    logger.info(f"Uploaded {csv_file_path} as {filename} to {hostname}.")
    return


def upload_to_s3(local_filename, filename, integration):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"icc/{integration}/{date_key}/{filename}"
    s3_client.upload_file(
        Filename=local_filename,
        Bucket=INVENTORY_BUCKET,
        Key=s3_key
    )
    logger.info(f"File {s3_key} uploaded to S3")


def convert_unified_to_icc(unified_inventory: list) -> pd.DataFrame:
    field_mappings = {
        "DealerId": "inv_dealer_integration_partner|provider_dealer_id",

        "VIN": "inv_vehicle|vin",
        "BodyType": "inv_vehicle|type",
        "Mileage": "inv_vehicle|mileage",
        "Make": "inv_vehicle|make",
        "Model": "inv_vehicle|model",
        "Year": "inv_vehicle|year",
        "isNew": "inv_vehicle|new_or_used",
        "Stock": "inv_vehicle|stock_num",

        "ListPrice": "inv_inventory|list_price",
        "CostPrice": "inv_inventory|cost_price",
        "FuelType": "inv_inventory|fuel_type",
        "Exteriorcolor": "inv_inventory|exterior_color",
        "Interiorcolor": "inv_inventory|interior_color",
        "DoorCount": "inv_inventory|doors",
        "Transmission": "inv_inventory|transmission",
        "PhotoUrl": "inv_inventory|photo_url",
        "DealerComments": "inv_inventory|comments",
        "Drivetrain": "inv_inventory|drive_train",
        "Cylinders": "inv_inventory|cylinders",
        "BodyStyle": "inv_inventory|body_style",
        "Interiormaterial": "inv_inventory|interior_material",
        "SourceDataDrivetrain": "inv_inventory|source_data_drive_train",
        "SourceDataInteriorMaterialDescription": "inv_inventory|source_data_interior_material_description",
        "SourceDataTransmission": "inv_inventory|source_data_transmission",
        "SourceDataTransmissionSpeed": "inv_inventory|source_data_transmission_speed",
        "TransmissionSpeed": "inv_inventory|transmission_speed",
        "BuildData": "inv_inventory|build_data",
        "HwyMPG": "inv_inventory|highway_mpg",
        "CityMPG": "inv_inventory|city_mpg",
        "VDP": "inv_inventory|vdp",
        "Trim": "inv_inventory|trim",
        "SpecialPrice": "inv_inventory|special_price",
        "Engine": "inv_inventory|engine",
        "EngineDisplacement": "inv_inventory|engine_displacement",
        "FactoryCertified": "inv_inventory|factory_certified",  # C if True, else null
    }
    rows = []
    inventory_columns = list(field_mappings.keys()) + ["StandardEquipment", "OptionalEquipment", "OptionDescription", "PriorityOptions"]

    for entry in unified_inventory:
        try:
            row = []
            for icc_field, unified_field in field_mappings.items():
                row.append(entry.get(unified_field))

            equipment_list = entry.get("inv_equipments|inv_equipments", [])
            standard_equipment = []
            optional_equipment = []
            for equipment in equipment_list:
                if equipment.get("inv_equipment|is_optional"):
                    optional_equipment.append(equipment.get("inv_equipment|equipment_description"))
                else:
                    standard_equipment.append(equipment.get("inv_equipment|equipment_description"))

            row.append("|".join(standard_equipment))
            row.append("|".join(optional_equipment))

            options_list = entry.get("inv_options|inv_options", [])
            option_description = []
            priority_options = []
            for option in options_list:
                if option.get("inv_option|is_priority"):
                    priority_options.append(option.get("inv_option|option_description"))
                else:
                    option_description.append(option.get("inv_option|option_description"))

            row.append("|".join(option_description))
            row.append("|".join(priority_options))

            rows.append(row)
        except Exception as e:
            logger.error(f"Error processing row: {entry} - {e}")
            raise Exception(f"Error converting unified format to ICC - {e}")

    icc_formatted_inventory = pd.DataFrame(rows, columns=inventory_columns)

    # Set FactoryCertified to C if True, else null
    icc_formatted_inventory["FactoryCertified"] = icc_formatted_inventory["FactoryCertified"].apply(lambda x: "C" if x else None)

    return icc_formatted_inventory


def get_ftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get FTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data["hostname"], secret_data["username"], secret_data["password"]


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record['body'])
        bucket = body["Records"][0]["s3"]["bucket"]["name"]
        key = body["Records"][0]["s3"]["object"]["key"]
        impel_dealer_id = key.split('/')[-1].split('.')[0]
        integration = key.split('/')[1]

        logger.info(f"Impel dealer id: {impel_dealer_id}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = loads(response['Body'].read())
        # logger.info(f"Content: {content}")

        icc_formatted_inventory = convert_unified_to_icc(content)
        logger.info(f"ICC formatted inventory: {icc_formatted_inventory.head()}")

        # Query RDS database for dealer FTP info for merch and AI
        rds_instance_obj = RDSInstance()
        ftp_data = rds_instance_obj.select_db_dealer_ftp_details(impel_dealer_id)
        logger.info(f"Dealer FTP details: {ftp_data}")
        if not ftp_data:
            logger.error(f"No FTP data found for dealer: {impel_dealer_id}")
            raise
        merch_dealer_id, ai_dealer_id, is_active_merch, is_active_ai = ftp_data[0]

        # Create temp file
        temp_dir = tempfile.TemporaryDirectory()
        csv_file_path = temp_dir.name + f'/{impel_dealer_id}.csv'
        icc_formatted_inventory.to_csv(csv_file_path, index=False)

        upload_to_s3(csv_file_path, f"{impel_dealer_id}.csv", integration)

        # Upload to FTP
        if is_active_merch:
            logger.info(f"Uploading to Merch FTP: {merch_dealer_id}")
            proccess_and_upload_to_ftp(icc_formatted_inventory, csv_file_path, merch_dealer_id, MERCH_FTP_KEY)

        # elif is_active_ai:
        #     logger.info(f"Uploading to AI FTP: {ai_dealer_id}")
        #     proccess_and_upload_to_ftp(icc_formatted_inventory, impel_dealer_id, ai_dealer_id, AI_FTP_KEY)
        else:
            logger.error(f"No active FTP found for dealer: {impel_dealer_id}")
            raise

        temp_dir.cleanup()

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Convert to ICC format and syndication to product FTPs."""
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
