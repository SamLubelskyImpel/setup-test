from datetime import datetime
import sqlalchemy as db
from orm.connection.make_db_uri import make_db_uri
from orm.models.carlabs.repair_order import RepairOrder
from orm.models.shared_dms import ServiceRepairOrder
import pandas
import logging
import os
import boto3
from transformers.repair_order import RepairOrderTransformer
from orm.connection.session import SQLSession
import traceback
from dataclasses import dataclass


FAILURES_QUEUE = os.environ.get('FAILURES_QUEUE')
BUCKET = os.environ.get('BUCKET')

SQS = boto3.client('sqs')
S3 = boto3.client('s3')

_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])


@dataclass
class RepairOrderETLProcess:

    limit: int
    day: datetime

    def __extract_from_carlabs(self):
        engine = db.create_engine(make_db_uri(db='CARLABS_ANALYTICS'))
        last_id = self.__get_last_processed()
        query = db.select(RepairOrder).where(
                    (RepairOrder.dataimport_creation_date >= self.day) &
                    (RepairOrder.id > last_id)
                ).order_by(
                    RepairOrder.id.asc()
                ).limit(self.limit+1)
        df = pandas.read_sql_query(sql=query, con=engine)
        records = df.to_dict('records')
        self.__has_more_data = len(records) > self.limit

        return records[:self.limit]

    def __transform(self, record: dict):
        data_source = record['ro_source']
        # TODO load relationships
        return RepairOrderTransformer(record).repair_order

    def __load_into_shared_dms(self, transformed: ServiceRepairOrder):
        ...
        # with SQLSession(db='SHARED_DMS') as session:
        #     session.add(transformed)

    def __save_progress(self, record: dict):
        S3.put_object(
            Body=str(record['id']),
            Bucket=BUCKET,
            Key='service_repair_order_progress'
        )

    def __send_to_failures_queue(self, record: dict, err: str):
        SQS.send_message(
            QueueUrl=FAILURES_QUEUE,
            MessageAttributes={
                'Table': {
                    'DataType': 'String',
                    'StringValue': 'repair_order'
                },
                'RecordId': {
                    'DataType': 'String',
                    'StringValue': str(record['id'])
                }
            },
            MessageBody=err
        )


    def __get_last_processed(self):
        try:
            return S3.get_object(
                Bucket=BUCKET,
                Key='service_repair_order_progress'
            )['Body'].read().decode('utf-8')
        except Exception:
            return 0

    def run(self):
        records = self.__extract_from_carlabs()
        n_transformed = 0
        failed = 0

        for r in records:
            try:
                _logger.info(f'processing {r["id"]}')
                self.__load_into_shared_dms(self.__transform(r))
                n_transformed += 1
            except Exception:
                _logger.exception('error')
                self.__send_to_failures_queue(r, traceback.format_exc())
                failed += 1
            finally:
                self.__save_progress(r)

        _logger.info(f'ETL loaded={n_transformed}, failed={failed}')

        return self.__has_more_data