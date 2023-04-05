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
    sys.argv, ["JOB_NAME", "db_name", "table_names", "catalog_connection"]
)

SM_CLIENT = boto3.client('secretsmanager')
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def _load_secret():
    secret_string = loads(SM_CLIENT.get_secret_value(
        SecretId='universal_integration/target_db'
    )['SecretString'])
    DB_CONFIG = {
        'target_database_name': secret_string['target_database_name'],
        'target_db_jdbc_url': secret_string['target_db_jdbc_url'],
        'target_db_user': secret_string['target_db_user'],
        'target_db_password': secret_string['target_db_password']
    }
    return DB_CONFIG

DB_CONFIG = _load_secret()

### unpack table_names


# datasource0 = glue_context.create_dynamic_frame.from_catalog(
#     database=args["db_name"],
#     table_name=args["table_name"],
#     transformation_ctx="datasource0",
# )

# datasource0.printSchema()