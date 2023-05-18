import psycopg2
import pandas
from mapper import get_dms_mapper_class, VehicleSaleMapper


connection = psycopg2.connect(
    user='integrator',
    password='CarLabs2022!',
    host='postgres.proxy.carlabs.com',
    port='45432',
    database='data_integrations'
)

final_result = []
offset = 0
limit = 100

while len(final_result) < 5000:
    select = f'''
        SELECT x.* FROM public."dataImports" x where x."dataType" = 'SALES' LIMIT {limit} OFFSET {offset}
    '''

    records = pandas.read_sql_query(select, connection).reset_index()
    offset += limit

    for i, r in records.iterrows():
        source =  r['dataSource'].upper()
        try:
            Mapper: VehicleSaleMapper = get_dms_mapper_class(source)()
            final_result.extend(Mapper.transform(r))
        except Exception:
            print(f'Error transforming ID {r["id"]}')

df = pandas.DataFrame.from_dict(final_result)
df.to_csv('sales_data.csv')

connection.close()