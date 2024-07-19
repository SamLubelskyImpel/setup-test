from datetime import datetime, date
import boto3
import os
from orm.models.shared_dms import DealerIntegrationPartner, IntegrationPartner, Dealer
from orm.connection.session import SQLSession
import logging
from typing import Union


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])

BUCKET = os.environ.get('BUCKET')
FAILURES_QUEUE = os.environ.get('FAILURES_QUEUE')


def parsed_date(date_format: str, raw_value: str) -> Union[date, int, None]:
    try:
        if date_format == 'year':
            return int(raw_value)
        else:
            return datetime.strptime(raw_value, date_format).date()
    except (ValueError, TypeError) as e:
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

class IntegrationNotActive(Exception):
    pass


def get_dealer_integration_partner_id(dealer_code: str, data_source: str) -> DealerIntegrationPartner:
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
        
        if dip and not dip.is_active:
            _logger.info(f"Found inactive DealerIntegrationPartner for dealer {dealer_code} and data source {data_source}.")
            raise IntegrationNotActive(f"DealerIntegrationPartner for dealer {dealer_code} and data source {data_source} is inactive.")

        if not dip:
            ip = session.query(
                IntegrationPartner
            ).filter(
                IntegrationPartner.impel_integration_partner_id.ilike(data_source)
            ).first()

            if not ip:
                _logger.info(f'Integration partner not found for')
                return None

            dealer = session.query(Dealer).filter(Dealer.impel_dealer_id == dealer_code).first()
            if not dealer:
                dealer = Dealer(
                    impel_dealer_id=dealer_code,
                )
                session.add(dealer)
                session.flush()

            new_dip = DealerIntegrationPartner(
                integration_partner_id=ip.id,
                dealer_id=dealer.id,
                dms_id=dealer_code,
                is_active=True,
                db_creation_date=datetime.utcnow()
            )

            session.add(new_dip)
            session.commit()
            return new_dip.id

        return dip.id
