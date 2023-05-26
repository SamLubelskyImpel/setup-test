import sqlalchemy as db
from orm.connection.make_db_uri import make_db_uri
from orm.connection.session import SQLSession
import pandas
from orm.models.carlabs.data_imports import DataImports
from orm.models.shared_dms import Vehicle, VehicleSale, Consumer, ServiceContract
from transformers.sales_history import CDKTransformer, DealertrackTransformer, DealervaultTransformer
from dataclasses import dataclass
import logging
import os
import boto3
import traceback
from datetime import datetime
from botocore.exceptions import ClientError


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])

FAILURES_QUEUE = os.environ.get('FAILURES_QUEUE')
BUCKET = os.environ.get('BUCKET')

SQS = boto3.client('sqs')
S3 = boto3.client('s3')


class DMSNotMapped(Exception):
    ...


@dataclass
class SalesHistoryTransformedData:
    vehicle_sale: VehicleSale
    vehicle: Vehicle
    consumer: Consumer
    service_contract: ServiceContract


@dataclass
class SalesHistoryETLProcess:

    limit: int
    day: datetime


    def __extract_from_carlabs(self):
        engine = db.create_engine(make_db_uri(db='CARLABS_DATA_INTEGRATIONS'))
        last_id = self.__get_last_processed()
        query = db.select(DataImports).where(
                    (DataImports.data_type == 'SALES') &
                    (DataImports.creation_date >= self.day) &
                    (DataImports.id > last_id)
                ).order_by(
                    DataImports.id.asc()
                ).limit(self.limit+1)
        df = pandas.read_sql_query(sql=query, con=engine)
        records = df.to_dict('records')
        self.__has_more_data = len(records) > self.limit

        for r in records[:self.limit]:
            _logger.info(f'record of id {r["id"]}')
            if isinstance(r['importedData'], list):
                _logger.info(f'has {len(r["importedData"])} inner records')
                if len(r['importedData']) == 0:
                    yield r
                for i in r['importedData']:
                    yield {
                        'id': r['id'],
                        'dealerCode': r['dealerCode'],
                        'importedData': i,
                        'dataSource': r['dataSource'],
                        'dataType': r['dataType'],
                        'creationDate': r['creationDate']
                    }
            else:
                yield r


    def __transform(self, record: dict):
        data_source = record['dataSource']
        if data_source == 'CDK':
            transformer = CDKTransformer(carlabs_data=record)
        elif data_source == 'DealerTrack':
            transformer = DealertrackTransformer(carlabs_data=record)
        elif data_source == 'DEALERVAULT':
            transformer = DealervaultTransformer(carlabs_data=record)
        else:
            raise DMSNotMapped

        return SalesHistoryTransformedData(
            vehicle_sale=transformer.vehicle_sale,
            vehicle=transformer.vehicle,
            consumer=transformer.consumer,
            service_contract=transformer.service_contract
        )


    def __load_into_shared_dms(self, transformed: SalesHistoryTransformedData):
        transformed.vehicle_sale.vehicle = transformed.vehicle
        transformed.vehicle_sale.consumer = transformed.consumer

        transformed.service_contract.vehicle = transformed.vehicle
        transformed.service_contract.consumer = transformed.consumer
        transformed.service_contract.sale = transformed.vehicle_sale

        # with SQLSession(db='SHARED_DMS') as session:
        #     session.add(transformed.vehicle)
        #     session.add(transformed.consumer)
        #     session.add(transformed.vehicle_sale)
        #     if transformed.vehicle_sale.has_service_contract:
        #         session.add(transformed.service_contract)


    def __send_to_failures_queue(self, record: dict, err: str):
        SQS.send_message(
            QueueUrl=FAILURES_QUEUE,
            MessageAttributes={
                'Table': {
                    'DataType': 'String',
                    'StringValue': 'dataImports'
                },
                'RecordId': {
                    'DataType': 'String',
                    'StringValue': str(record['id'])
                }
            },
            MessageBody=err
        )


    def __save_progress(self, record: dict):
        S3.put_object(
            Body=str(record['id']),
            Bucket=BUCKET,
            Key='sales_history_progress'
        )


    def __get_last_processed(self):
        try:
            return S3.get_object(
                Bucket=BUCKET,
                Key='sales_history_progress'
            )['Body'].read().decode('utf-8')
        except Exception:
            return 0


    def run(self):
        records = self.__extract_from_carlabs()
        n_transformed = 0
        failed = 0

        for r in records:
            try:
                self.__load_into_shared_dms(self.__transform(r))
                n_transformed += 1
            except Exception:
                self.__send_to_failures_queue(r, traceback.format_exc())
                failed += 1
            finally:
                self.__save_progress(r)

        _logger.info(f'ETL loaded={n_transformed}, failed={failed}')

        return self.__has_more_data
