"""Rey Rey ETL Job."""

import sys
from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from json import loads

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "db_name", "catalog_table_names", "catalog_connection", "environment"]
)

SM_CLIENT = boto3.client('secretsmanager')
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)
isprod = args["environment"] == 'prod'


class ReyReyUpsertJob:

    def __init__(self, args):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(args['JOB_NAME'], args)
        self.DB_CONFIG = _load_secret()
        #each catalog_table_names(expecting 2) should have an upsert order.
        self.catalog_table_names = args['catalog_table_names']
        self.upsert_table_order = get_upsert_table_order()


    def get_upsert_table_order():
        
        upsert_table_order = {}
        for name in self.catalog_table_names:
            if 'fi_closed_deal' in name:
                upsert_table_order[name] = ['consumer', 'vehicle', 'vehicle_sale']
            elif 'repair_order' in name:
                upsert_table_order[name] = ['dealer', 'vehicle', 'consumer', 'service_repair_order']

        return upsert_table_order


    def _load_secret():
        secret_string = loads(SM_CLIENT.get_secret_value(
            SecretId='prod/DMSDB' if isprod else 'stage/DMSDB' 
        )['SecretString'])
        DB_CONFIG = {
            'target_database_name': secret_string["db_name"],
            'target_db_jdbc_url': secret_string['jdbc_url'],
            'target_db_user': secret_string['user'],
            'target_db_password': secret_string['password']
        }
        return DB_CONFIG

    def apply_mapping(self, tablename):
        pass


    def read_data_from_catalog(self, database, table_name):
        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            transformation_ctx="datasource0"
        )


    def run(self, database, table_names):

        for catalog_table in self.catalog_table_names:

            for table_name in self.upsert_table_order[catalog_table]:
                datasource0 = self.read_data_from_catalog(database=database, table_name=table_name)

                # Apply mappings to the DynamicFrame
                df_transformed = self.apply_mappings(datasource0, table_name)

                # Convert DynamicFrame to DataFrame
                df = df_transformed.toDF()

                # Perform upsert based on the table_name
                if table_name == 'consumer':
                    df.rdd.foreachPartition(self.upsert_consumer_partition)
                elif table_name == 'vehicle':
                    df.rdd.foreachPartition(self.upsert_vehicle_partition)
                elif table_name == 'vehicle_sale':
                    df.rdd.foreachPartition(self.upsert_vehicle_sale_partition)
                elif table_name == 'dealer':
                    df.rdd.foreachPartition(self.upsert_dealer_partition)
                elif table_name == 'service_repair_order':
                    df.rdd.foreachPartition(self.upsert_deal_partition)
                else:
                    raise ValueError(f"Invalid table name: {table_name}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = ReyReyUpsertJob(args)    
    job.run(database=args["db_name"])