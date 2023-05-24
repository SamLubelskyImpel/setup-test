import sqlalchemy as db
from orm.connection.make_db_uri import make_db_uri
from orm.connection.session import SQLSession
import pandas
from orm.models.carlabs import DataImports
from orm.models.shared_dms import Vehicle, VehicleSale, Consumer, ServiceContract
from transformers.sales_history import CDKTransformer, DealertrackTransformer, DealervaultTransformer
from dataclasses import dataclass


class DMSNotMapped(Exception):
    ...


@dataclass
class TransformedData:
    vehicle_sale: VehicleSale
    vehicle: Vehicle
    consumer: Consumer
    service_contract: ServiceContract


def extract_from_carlabs(limit: int = 100):
    engine = db.create_engine(make_db_uri(db='CARLABS'))
    df = pandas.read_sql_query(
        sql=db.select(DataImports).where(
            (DataImports.data_type == 'SALES')
        ).limit(limit),
        con=engine)
    breakpoint()

    for r in df.to_dict('records'):
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


def transform(record: dict):
    data_source = record['dataSource']
    if data_source == 'CDK':
        transformer = CDKTransformer(carlabs_data=record)
    elif data_source == 'DealerTrack':
        transformer = DealertrackTransformer(carlabs_data=record)
    elif data_source == 'DEALERVAULT':
        transformer = DealervaultTransformer(carlabs_data=record)
    else:
        raise DMSNotMapped

    return TransformedData(
        vehicle_sale=transformer.vehicle_sale,
        vehicle=transformer.vehicle,
        consumer=transformer.consumer,
        service_contract=transformer.service_contract
    )


def load_into_shared_dms(transformed: TransformedData):
    transformed.vehicle_sale.vehicle = transformed.vehicle
    transformed.vehicle_sale.consumer = transformed.consumer

    transformed.service_contract.vehicle = transformed.vehicle
    transformed.service_contract.consumer = transformed.consumer
    transformed.service_contract.sale = transformed.vehicle_sale

    with SQLSession(db='SHARED_DMS') as session:
        session.add(transformed.vehicle)
        session.add(transformed.consumer)
        session.add(transformed.vehicle_sale)
        if transformed.vehicle_sale.has_service_contract:
            session.add(transformed.service_contract)


import logging
logger = logging.getLogger()

records = extract_from_carlabs()
n_transformed = 0
failed = 0

for r in records:
    try:
        load_into_shared_dms(transform(r))
        n_transformed += 1
    except Exception as e:
        logger.exception(f'failed to transform {r["id"]}')
        failed += 1

print(f'loaded {n_transformed}, failed {failed}')