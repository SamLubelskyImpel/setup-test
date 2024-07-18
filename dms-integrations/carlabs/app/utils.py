from datetime import datetime, date
import boto3
import os
from orm.models.shared_dms import DealerIntegrationPartner, IntegrationPartner, Dealer
from orm.models.carlabs import DataImports
from orm.connection.session import SQLSession
import logging


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])

BUCKET = os.environ.get('BUCKET')
FAILURES_QUEUE = os.environ.get('FAILURES_QUEUE')


def parsed_date(date_format: str, raw_date: str) -> date:
    try:
        return datetime.strptime(raw_date, date_format)
    except Exception:
        return None


def parsed_int(v) -> int:
    if isinstance(v, str):
        v = v.replace(',', '')
    return int(v) if v else None


def save_progress(id: str, key: str):
    S3 = boto3.client('s3')
    S3.put_object(
        Body=str(id),
        Bucket=BUCKET,
        Key=key
    )


def load_progress(key: str):
    S3 = boto3.client('s3')
    try:
        return S3.get_object(
            Bucket=BUCKET,
            Key=key
        )['Body'].read().decode('utf-8')
    except Exception:
        return 0


def publish_failure(record: dict, err: str, table: str):
    SQS = boto3.client('sqs')
    SQS.send_message(
        QueueUrl=FAILURES_QUEUE,
        MessageAttributes={
            'Table': {
                'DataType': 'String',
                'StringValue': table
            },
            'RecordId': {
                'DataType': 'String',
                'StringValue': str(record['id'])
            }
        },
        MessageBody=err
    )


class DealerIntegrationNotFound(Exception):
    ...


def get_dealer_integration_partner_id(dealer_code: str, data_source: str) -> int:
    with SQLSession(db='SHARED_DMS') as session:
        dip = session.query(
            DealerIntegrationPartner
        ).join(
            IntegrationPartner,
            DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
        ).join(
            Dealer,
            DealerIntegrationPartner.dealer_id == Dealer.id
        ).filter(
            (IntegrationPartner.impel_integration_partner_id.ilike(data_source)) &
            (DealerIntegrationPartner.is_active) &
            (Dealer.impel_dealer_id == dealer_code)
        ).first()
        if not dip:
            _logger.info('START OF IF NOT DIP')
            # If dealer is not found, dynamically insert them here
            with SQLSession(db='CARLABS_DATA_INTEGRATIONS') as dms_session:
                record = dms_session.query(
                    DataImports.dealerCode.distinct(),
                    DataImports.dataSource
                ).filter(
                    DataImports.dealerCode == dealer_code
                ).filter(
                    DataImports.dataType == 'SALES'
                ).first()

                if not record:
                    _logger.info(f"No active dealer {dealer_code} found in dataImports.")
                
                ip = dms_session.query(
                    IntegrationPartner
                ).filter(
                    IntegrationPartner.impel_integration_partner_id.ilike(record[1])
                ).first()

                if not ip:
                    _logger.info(f'Integration partner not found for {record}')
                    return None

                # Create new dealer and DealerIntegrationPartner if IntegrationPartner is found
                new_dealer = Dealer(
                    impel_dealer_id=dealer_code,
                    db_creation_date=datetime.utcnow()
                )

                dms_session.add(new_dealer)
                dms_session.commit()

                new_dip = DealerIntegrationPartner(
                    integration_partner_id=ip.id,
                    dealer_id=new_dealer.id,
                    dms_id=record[0],
                    is_active=True,
                    db_creation_date=datetime.utcnow()
                )

                dms_session.add(new_dip)
                dms_session.commit()
                return new_dip.id

        return dip.id