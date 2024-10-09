import logging
import os
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from typing import Any, List, Dict
import boto3
from json import loads
from sqlalchemy.orm import Session
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer_profile import ConsumerProfile
from datetime import datetime
import urllib.parse

# Set up logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

# Initialize S3 client
s3 = boto3.client('s3')

FORD_DIRECT_SUCCESS_STATUS = '1'

def fetch_s3_file(bucket_name: str, file_key: str) -> List[str]:
    """Download the S3 file and return its contents as lines."""
    decoded_key = urllib.parse.unquote(file_key)
    download_path = f'/tmp/{os.path.basename(decoded_key)}'
    s3.download_file(bucket_name, decoded_key, download_path)
    logger.info(f'Downloaded file from S3: {download_path}')
    
    # Read file contents as list of rows
    with open(download_path, 'r') as f:
        return f.readlines()

def parse_pii_file(file_lines: List[str]) -> List[Dict[str, Any]]:
    """Parse the PII file into a list of dictionaries (for each consumer row)."""
    header, *data_lines = file_lines  
    fields = header.strip().split("|^|")
    
    parsed_rows = []
    for line in data_lines:
        line_data = line.strip().split("|^|")
        row = dict(zip(fields, line_data))
        
        if row.get('MATCH_RESULT') == FORD_DIRECT_SUCCESS_STATUS:
            parsed_rows.append(row)
    
    return parsed_rows

def merge_consumer_rows(pii_rows):
    """Merge multiple PII rows into a single consumer dictionary per EXT_CONSUMER_ID."""
    merged_data = {}
    for row in pii_rows:
        ext_consumer_id = row['EXT_CONSUMER_ID']
        if ext_consumer_id not in merged_data:
            merged_data[ext_consumer_id] = {
                'EXT_CONSUMER_ID': ext_consumer_id,
                'DEALER_IDENTIFIER': row['DEALER_IDENTIFIER'],
                'FDGUID': row['FDGUID'],
                'FDDGUID': row['FDDGUID'],
                'STATUS': row['STATUS'],
                'pacode': row['pacode'],
                'MATCH_RESULT': row['MATCH_RESULT'],
                'fName': None,
                'lName': None,
                'email': None,
                'phone': None
            }
        else:
            # Check for FDGUID consistency and log if there's a mismatch
            if row['FDGUID'] != merged_data[ext_consumer_id]['FDGUID']:
                logger.warning(
                    f"Multiple FDGUIDs detected for {ext_consumer_id}. "
                    f"Using {merged_data[ext_consumer_id]['FDGUID']}, ignoring {row['FDGUID']}"
                )
        
        pii_type = row['PII_Type']
        pii_value = row['PII']
        if pii_type == 'email':
            merged_data[ext_consumer_id]['email'] = pii_value
        elif pii_type == 'lName':
            merged_data[ext_consumer_id]['lName'] = pii_value
        elif pii_type == 'fName':
            merged_data[ext_consumer_id]['fName'] = pii_value
        elif pii_type == 'phone':
            merged_data[ext_consumer_id]['phone'] = pii_value
    
    return merged_data


def upsert_consumer_profile(merged_data: Dict[str, Any], session: Session) -> Dict[str, List[int]]:
    """Upsert the consumer profiles into the database and return IDs of updated or created records."""
    updated_ids = []
    created_ids = []
    
    for consumer_id, consumer_data in merged_data.items():
        # use the pacode as the integration_partner_id
        integration_partner_id = consumer_data.get('pacode')
        
        # Query for the existing profile using consumer_id and integration_partner_id
        existing_profile = session.query(ConsumerProfile).filter_by(
            consumer_id=consumer_id, 
            integration_partner_id=integration_partner_id
        ).first()
        
        if existing_profile:
            # Update existing profile
            existing_profile.cdp_master_consumer_id = consumer_data.get('FDGUID', existing_profile.cdp_master_consumer_id)
            existing_profile.cdp_dealer_consumer_id = consumer_data.get('FDDGUID', existing_profile.cdp_dealer_consumer_id)
            existing_profile.db_update_date = datetime.utcnow()
            session.commit()
            
            updated_ids.append(existing_profile.id)
        else:
            # Insert new profile
            new_profile = ConsumerProfile(
                consumer_id=consumer_id,
                integration_partner_id=integration_partner_id,
                cdp_master_consumer_id=consumer_data.get('FDGUID'),
                cdp_dealer_consumer_id=consumer_data.get('FDDGUID'),
                db_creation_date=datetime.utcnow(),
            )
            session.add(new_profile)
            session.commit()
            
            created_ids.append(new_profile.id)
    
    return {
        'updated': updated_ids,
        'created': created_ids
    }

    
def record_handler(record: Dict[str, Any]):
    """Process an individual record from the SQS event."""
    logger.info(f'Record: {record}')
    
    try:
        event = loads(record['body'])
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        # Fetch and parse S3 file
        file_lines = fetch_s3_file(bucket_name, file_key)
        pii_rows = parse_pii_file(file_lines)
        
        # Merge rows by consumer ID
        merged_data = merge_consumer_rows(pii_rows)
        logger.info(f"merged_data:{merged_data}")

        # Upsert into the database
        with DBSession() as session:
            result = upsert_consumer_profile(merged_data, session)
            # Log the IDs of updated and created profiles
            if result['updated']:
                logger.info(f"Updated profiles with IDs: {result['updated']}")
            if result['created']:
                logger.info(f"Created profiles with IDs: {result['created']}")
        
        logger.info(f'Successfully processed file: {file_key}')
    
    except Exception as e:
        logger.exception(f'Error processing record: {str(e)}')
        raise

def lambda_handler(event: Any, context: Any):
    """Main Lambda handler function for processing SQS events."""
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
        logger.exception(f"Error processing event: {str(e)}")
        raise
