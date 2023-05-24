import sqlalchemy as db
from orm.connection.make_db_uri import make_db_uri
from orm.connection.session import SQLSession
import pandas
from orm.models.carlabs import DataImports
from orm.models.shared_dms import Vehicle, VehicleSale, Consumer, ServiceContract
from transformers.sales_history import CDKTransformer, DealertrackTransformer, DealervaultTransformer
from dataclasses import dataclass
import logging
import os

_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])


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
    offset: int


    def __extract_from_carlabs(self):
        engine = db.create_engine(make_db_uri(db='CARLABS_DATA_INTEGRATIONS'))
        df = pandas.read_sql_query(
            # TODO add condition to get the data of the day only
            sql=db.select(DataImports).where(
                (DataImports.data_type == 'SALES')
            ).limit(self.limit+1).offset(self.offset),
            con=engine)

        records = df.to_dict('records')

        self.__has_more_data = len(records) > self.limit

        for r in records[:self.limit]:
            if isinstance(r['importedData'], list):
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


    def run(self):
        records = self.__extract_from_carlabs()
        n_transformed = 0
        failed = 0

        for r in records:
            try:
                self.__load_into_shared_dms(self.__transform(r))
                n_transformed += 1
            except Exception:
                _logger.exception(f'failed to transform {r["id"]}')
                failed += 1

        _logger.info(f'loaded {n_transformed}, failed {failed} (offset={self.offset})')

        return self.__has_more_data