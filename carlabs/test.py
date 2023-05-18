from app.orm.connection.session import SQLSession, engine
from app.orm.models.carlabs import DataImports
from app.transformers.dealertrack import DealertrackTransformer
from app.transformers.cdk import CDKTransformer
from app.transformers.dealervault import DealervaultTransformer
import pandas
import sqlalchemy as db
from app.orm.connection.make_db_uri import make_db_uri


engine = db.create_engine(make_db_uri())

# df = pandas.read_sql_query(
#     sql=db.select(DataImports).where(DataImports.data_source == 'CDK', DataImports.data_type == 'SALES').limit(2),
#     con=engine)

# for i, record in df.iterrows():
#     copy = record.copy()
#     copy['importedData'] = record['importedData'][0]
#     transformed = CDKTransformer(carlabs_data=copy)
#     vehicle_sale = transformed.vehicle_sale
#     consumer = transformed.consumer
#     breakpoint()

# df = pandas.read_sql_query(
#     sql=db.select(DataImports).where(DataImports.data_source == 'DealerTrack', DataImports.data_type == 'SALES').limit(2),
#     con=engine)

# for i, record in df.iterrows():
#     copy = record.copy()
#     transformed = DealertrackTransformer(carlabs_data=copy)
#     vehicle_sale = transformed.vehicle_sale
#     consumer = transformed.consumer
#     breakpoint()

df = pandas.read_sql_query(
    sql=db.select(DataImports).where(DataImports.data_source == 'DEALERVAULT', DataImports.data_type == 'SALES').limit(2),
    con=engine)

for i, record in df.iterrows():
    copy = record.copy()
    copy['importedData'] = record['importedData'][0]
    transformed = DealervaultTransformer(carlabs_data=copy)
    vehicle_sale = transformed.vehicle_sale
    consumer = transformed.consumer
    breakpoint()