"""Tekion ETL Job."""

import logging
import sys
import uuid
from datetime import datetime
from json import dumps, loads

import boto3
import psycopg2
import pyspark
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self, is_prod, integration):
        self.integration = integration
        self.is_prod = is_prod
        self.schema = f"{'prod' if self.is_prod else 'stage'}"
        self.rds_connection = self.get_rds_connection()

    def get_rds_connection(self):
        """Get connection to RDS database."""
        sm_client = boto3.client("secretsmanager")
        secret_string = loads(
            sm_client.get_secret_value(
                SecretId="prod/DMSDB" if self.is_prod else "test/DMSDB"
            )["SecretString"]
        )
        return psycopg2.connect(
            user=secret_string["user"],
            password=secret_string["password"],
            host=secret_string["host"],
            port=secret_string["port"],
            database=secret_string["db_name"],
        )

    def execute_rds(self, query_str):
        """Execute query on RDS and return cursor."""
        cursor = self.rds_connection.cursor()
        cursor.execute(query_str)
        return cursor

    def commit_rds(self, query_str):
        """Execute and commit query on RDS and return cursor."""
        cursor = self.execute_rds(query_str)
        self.rds_connection.commit()
        return cursor

    def get_multi_insert_query(self, records, columns, table_name, additional_query=""):
        """Commit several records to the database.
        columns is an array of strings of the column names to insert into
        records is an array of tuples in the same order as columns
        table_name is the 'schema."table_name"'
        additional_query is any query text to append after the insertion
        """
        if len(records) >= 1:
            cursor = self.rds_connection.cursor()
            values_str = f"({', '.join('%s' for _ in range(len(records[0])))})"
            args_str = ",".join(
                cursor.mogrify(values_str, x).decode("utf-8") for x in records
            )
            columns_str = ", ".join(columns)
            query = f"""INSERT INTO {table_name} ({columns_str}) VALUES {args_str} {additional_query}"""
            return query
        else:
            return None

    def get_insert_query_from_df(self, df, table, additional_query=""):
        """Get query from df where df column names are db column names for the given table."""
        column_names = df.columns
        column_data = []
        for row in df.collect():
            column_data.append(tuple(row))
        table_name = f'{self.schema}."{table}"'
        query = self.get_multi_insert_query(
            column_data, column_names, table_name, additional_query
        )
        return query

    def select_db_dealer_id(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_id_query = f"""
            select d.id from {self.schema}."dealer" d 
            join {self.schema}."integration_partner" i on d.integration_id = i.id 
            where d.dms_id = '{dms_id}' and i.name = '{self.integration}';"""
        results = self.execute_rds(db_dealer_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No dealer {dms_id} found with query {db_dealer_id_query}."
            )
        else:
            return results[0]

    def insert_consumer(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into consumer table and return created ids."""
        desired_consumer_columns = mappings[catalog_name]["consumer"].keys()
        actual_consumer_columns = [
            x for x in desired_consumer_columns if x in dealer_df.columns
        ]
        actual_consumer_columns.append("dealer_id")

        consumer_df = dealer_df.select(actual_consumer_columns)
        insert_consumer_query = self.get_insert_query_from_df(
            consumer_df, "consumer", "RETURNING id"
        )

        results = self.commit_rds(insert_consumer_query)
        
        logging.warning(f"RESULTS: {results}")
        
        if results is None:
            return []
        inserted_consumer_ids = [x[0] for x in results.fetchall()]
        return inserted_consumer_ids


class TekionUpsertJob:
    """Create object to perform ETL."""

    def __init__(self, job_id, args):
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(args["JOB_NAME"], args)
        self.job_id = job_id
        self.catalog_table_names = args["catalog_table_names"].split(",")
        self.dlq_url = args["dlq_url"]
        self.database = args["db_name"]
        self.is_prod = args["environment"] == "prod"
        self.integration = "tekion"
        # self.bucket_name = (
        #     f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        # )
        self.bucket_name = "s3://integrations-etl-test/tekion/results/"
        self.rds = RDSInstance(self.is_prod, self.integration)
        self.mappings = {
            "tekioncrawlerdb_customer": {
                # TODO: get dealer_id from the onboarding team
                "dealer": {"dms_id": "techmotors_4"},
                "consumer": {
                    "first_name": "data.firstname",
                    "last_name": "data.lastname",
                    "email": "data.email",
                    "phones": "data.phones",
                    "cell_phone": "data.phones",
                    "home_phone": "data.phones",
                    "addresses": "data.addresses",
                    "city": "data.addresses",
                    "state": "data.addresses",
                    "postal_code": "data.addresses",
                    "email_optin_flag": "data.communicationPreferences.email",
                    "phone_optin_flag": "data.communicationPreferences.call",
                    "postal_mail_optin_flag": "data.communicationPreferences.postalMail",
                    "sms_optin_flag": "data.communicationPreferences.text"
                }
            }
        }
 
    def select_columns(self, df, table_to_mappings):
        """Select valid db columns from a dataframe using dms column mappings, log and skip missing data."""
        ignore_columns = []
        selected_columns = []
        selected_column_names = []
        for db_columns_to_dms_columns in table_to_mappings.values():
            for db_column, dms_column in db_columns_to_dms_columns.items():
                try:
                    df.select(dms_column)
                except pyspark.sql.utils.AnalysisException:
                    logger.warning(
                        f"Column: {db_column} with mapping: {dms_column} not found, default to null."
                    )
                    ignore_columns.append(db_column)

            for db_column, dms_column in db_columns_to_dms_columns.items():
                if (
                    db_column not in ignore_columns
                    and db_column not in selected_column_names
                ):
                    selected_columns.append(F.col(dms_column).alias(db_column))
                    selected_column_names.append(db_column)
                    
        logging.warning(f"Selected columns: {selected_columns}")
        return df.select(selected_columns)

    def apply_mappings(self, df, catalog_name):
        """Map the raw data to the unified column and return as a dataframe."""
        if "tekioncrawlerdb_customer" == catalog_name:
            data_column_name = "data"
        else:
            raise RuntimeError(f"Unexpected catalog {catalog_name}")

        # Log data with null values
        null_data = df.filter(F.col(f"{data_column_name}").isNull()).select(
            "Year",
            "Month",
            "Date",
        )
        null_data_json = null_data.toJSON().collect()
        logging.warning(f"Skip processing null data: {null_data_json}")

        # Log and select data without null values
        valid_data = df.filter(F.col(f"{data_column_name}").isNotNull()).select(
            "Year",
            "Month",
            "Date",
            F.explode(f"{data_column_name}").alias(data_column_name),
        )

        valid_data_json = (
            valid_data.select(
                "Year",
                "Month",
                "Date",
            )
            .toJSON()
            .collect()
        )

        logging.warning(f"Processing data: {valid_data_json}")

        # Select columns raw data by mapping
        table_data = self.select_columns(valid_data, self.mappings[catalog_name])
        return table_data
  
    def format_df(self, df, catalog_name):
        """Format the raw data to match the database schema."""
        starting_count = df.count()
        df = df.withColumn("dms_id", F.lit("techmotors_4"))
        if "phones" in df.columns:
            df = df.withColumn("cell_phone", F.expr("filter(phones, phone -> phone.phoneType == 'MOBILE')")[0]["number"])
            df = df.withColumn("home_phone", F.expr("filter(phones, phone -> phone.phoneType == 'HOME')")[0]["number"])
            df = df.drop("phones")
        if "addresses" in df.columns:
            df = df.withColumn("city", F.expr("IF(SIZE(addresses) > 0, addresses[0]['city'], NULL)"))
            df = df.withColumn("state", F.expr("IF(SIZE(addresses) > 0, addresses[0]['state'], NULL)"))
            df = df.withColumn("postal_code", F.expr("IF(SIZE(addresses) > 0, addresses[0]['zip'], NULL)"))
            df = df.drop("addresses")
        if "email_optin_flag" in df.columns:
            df = df.withColumn("email_optin_flag", F.when((F.col("email_optin_flag.serviceUpdates") == True) | (F.col("email_optin_flag.marketing") == True), True).otherwise(False))
        if "phone_optin_flag" in df.columns:
            df = df.withColumn("phone_optin_flag", F.when((F.col("phone_optin_flag.serviceUpdates") == True) | (F.col("phone_optin_flag.marketing") == True), True).otherwise(False))
        if "sms_optin_flag" in df.columns:
            df = df.withColumn("sms_optin_flag", F.when((F.col("sms_optin_flag.serviceUpdates") == True) | (F.col("sms_optin_flag.marketing") == True), True).otherwise(False))
        if "postal_mail_optin_flag" in df.columns:
            df = df.withColumn("postal_mail_optin_flag", F.when((F.col("postal_mail_optin_flag.serviceUpdates") == True) | (F.col("postal_mail_optin_flag.marketing") == True), True).otherwise(False))
        
        if starting_count != df.count():
            raise RuntimeError(
                f"Error formatting lost data from {starting_count} rows to {df.count()}"
            )
        return df
    
    def upsert_df(self, df, catalog_name):
        """Upsert dataframe to RDS table."""
        insert_count = 0
        dealers = df.select("dms_id").distinct().collect()
        for dealer in dealers:
            dealer_df = df.filter(df.dms_id == dealer.dms_id)
            db_dealer_id = None
            try:
                db_dealer_id = self.rds.select_db_dealer_id(dealer.dms_id)
                dealer_df = dealer_df.withColumn("dealer_id", F.lit(db_dealer_id))
                if catalog_name == "tekioncrawlerdb_customer":
                    # Vehicle sale must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    logger.warning(
                        f"Added {count} rows to consumer for dealer {db_dealer_id}"
                    )
                    insert_count += count
            except Exception:
                logger.exception(
                    f"""Error: db_dealer_id {db_dealer_id}, dms_id {dealer.dms_id}, catalog {catalog_name} {df.schema}"""
                )

                schema_json = loads(dealer_df.schema.json())
                data_json = [row.asDict(recursive=True) for row in dealer_df.collect()]
                s3_key = f"{self.integration}/errors/{datetime.now().strftime('%Y-%m-%d')}/{dealer.dms_id}_{self.job_id}/{uuid.uuid4().hex}.json"
                s3_client = boto3.client("s3")
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=dumps({"schema": schema_json, "data": data_json}),
                    ContentType="application/json",
                )

                # To rebuild df from the json data
                # df = spark.read.schema(StructType.fromJson(schema_json)).json(spark.sparkContext.parallelize([json.dumps(data_json)]))

                # sqs_client = boto3.client("sqs")
                # sqs_client.send_message(
                #     QueueUrl=self.dlq_url,
                #     MessageBody=dumps({"bucket": self.bucket_name, "key": s3_key}),
                # )

        return insert_count

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            transformation_ctx = "datasource36_" + catalog_name
            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=transformation_ctx,
            )
            if datasource.count() != 0:
                df = (
                    datasource.toDF()
                    .withColumnRenamed("partition_0", "Year")
                    .withColumnRenamed("partition_1", "Month")
                    .withColumnRenamed("partition_2", "Date")
                )

                # Get the base fields in a df via self.mappings
                mapped_df = self.apply_mappings(df, catalog_name)

                # Format necessary base fields to get a standardized df format
                formatted_df = self.format_df(mapped_df, catalog_name)
                
                # Convert the DataFrame back to a DynamicFrame
                # dynamic_frame = DynamicFrame.fromDF(formatted_df, self.glue_context, "dynamic_frame")
                
                # # Write out the result dataset to S3 (testing)
                # self.glue_context.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", connection_options={"path": self.bucket_name}, format="json")

                # If you've properly formatted your df the next function should run without needing modifications per integration
                upsert_count = self.upsert_df(formatted_df, catalog_name)
                logger.info(
                    f"Added {upsert_count} total rows to primary table for {catalog_name}."
                )
            else:
                logger.error("There is no new data for the job to process.")

        self.job.commit()


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "db_name",
            "catalog_table_names",
            "catalog_connection",
            "environment",
            "dlq_url",
        ],
    )

    job_id = args["JOB_RUN_ID"]
    logging.basicConfig(
        format=str(job_id) + " %(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        job = TekionUpsertJob(job_id, args)
        job.run()
    except Exception:
        logger.exception("Error running Tekion ETL.")
        raise
