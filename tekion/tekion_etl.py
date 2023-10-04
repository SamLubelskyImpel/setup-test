"""Tekion ETL Job."""

import logging
import sys
from json import dumps, loads

import boto3
import psycopg2
import pyspark
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from awsglue.gluetypes import ArrayType, ChoiceType, StructType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType


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

    def get_table_names(self):
        """ Get a list of table names in the database. """
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        cursor.execute(query)
        table_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return table_names

    def get_table_column_names(self, table_name):
        """ Get a list of column names in the given database table. """
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        cursor.execute(query)
        column_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return column_names

    def get_unified_column_names(self):
        """ Get a list of column names from all database tables in unified format. """
        unified_column_names = []
        tables = self.get_table_names()
        for table in tables:
            columns = self.get_table_column_names(table)
            for column in columns:
                unified_column_names.append(f"{table}|{column}")
        return unified_column_names


class DynamicFrameResolver:
    """Resolve choices from a glue dynamic frame."""

    def __init__(self, glue_context):
        self.glue_context = glue_context

    def get_resolved_choices(self, unresolved_columns):
        """Determine how to handle unresolved columns"""
        resolve_spec = []
        for unresolved_column in unresolved_columns:
            choices = unresolved_column[1].keys()
            field_name = unresolved_column[0]
            if "struct" in choices and "array" in choices:
                resolve_spec.append((field_name, "make_cols"))
            else:
                resolve_spec.append((field_name, "cast:string"))
        return resolve_spec

    def get_unresolved_columns(self, schema):
        """Check dynamic frame for unresolved data types."""
        unresolved_columns = []

        def _recursion(field_map, unresolved_column_names, build_name=""):
            for field in field_map.values():
                field_name = f"{build_name + '.' if build_name else ''}{field.name if field.name else ''}"

                if isinstance(field.dataType, StructType):
                    _recursion(
                        field.dataType.field_map, unresolved_column_names, field_name
                    )

                if isinstance(field.dataType, ArrayType):
                    if isinstance(field.dataType.elementType, StructType):
                        field_name += "[]"
                        _recursion(
                            field.dataType.elementType.field_map,
                            unresolved_column_names,
                            field_name,
                        )

                    if isinstance(field.dataType.elementType, ChoiceType):
                        unresolved_column_names.append(
                            (field_name, field.dataType.elementType.choices)
                        )

                if isinstance(field.dataType, ChoiceType):
                    unresolved_column_names.append((field_name, field.dataType.choices))

        _recursion(schema.field_map, unresolved_columns)

        # Sort most nested to least nested
        unresolved_columns = sorted(
            unresolved_columns, key=lambda x: len(x[0].split(".")), reverse=False
        )

        return unresolved_columns

    def resolve_choices(self, df, name):
        """Resolve nested choices inside a dynamicframe."""
        i = 0
        max_resolutions = 50
        all_resolved_specs = []
        unresolved_columns = self.get_unresolved_columns(df.schema())
        while len(unresolved_columns) > 0:
            if i > max_resolutions:
                raise RuntimeError(
                    f"Unable to resolve {name} with {unresolved_columns}"
                )
            resolved_spec = self.get_resolved_choices(unresolved_columns)
            all_resolved_specs += resolved_spec
            df = df.resolveChoice(specs=resolved_spec)
            unresolved_columns = self.get_unresolved_columns(df.schema())
            i += 1
        return df


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
        self.region = args["region"]
        self.is_prod = args["environment"] == "prod"
        self.integration = "tekion"
        self.bucket_name = (
            f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        )
        self.rds = RDSInstance(self.is_prod, self.integration)
        self.resolver = DynamicFrameResolver(self.glue_context)
        self.required_columns = []
        self.metadata_tables = ["service_repair_order", "vehicle_sale", "vehicle", "consumer"]
        self.mappings = {
            "tekioncrawlerdb_fi_closed_deal": {
                "dealer_integration_partner": {"dms_id": "dms_id"},
                "consumer": {
                    "customers": "customers",
                    "first_name": "customers.firstName",
                    "last_name": "customers.lastName",
                    "email": "customers.emails",
                    "phones": "customers.phones",
                    "cell_phone": "customers.phones",
                    "home_phone": "customers.phones",
                    "addresses": "customers.addresses",
                    "city": "customers.addresses.city",
                    "state": "customers.addresses.state",
                    "postal_code": "customers.addresses.zip",
                    "email_optin_flag": "customers.communicationPreferences.email",
                    "phone_optin_flag": "customers.communicationPreferences.call",
                    "postal_mail_optin_flag": "customers.communicationPreferences.mail",
                    "sms_optin_flag": "customers.communicationPreferences.text"
                },
                "vehicle": {
                    "vehicles": "vehicles",
                    "vin": "vehicles.vin",
                    "type": "vehicles.type",
                    "vehicle_class": "vehicles.trimDetails.bodyClass",
                    "mileage": "vehicles.mileage.value",
                    "make": "vehicles.make",
                    "model": "vehicles.model",
                    "year": "vehicles.year",
                    "new_or_used": "vehicles.stockType"
                },
                "vehicle_sale": {
                    "sale_date": "contractDate",
                    "listed_price": "vehicles.pricing.retailPrice.amount",
                    "sales_tax": "dealPayment.termPayment.totals.taxAmount.amount",
                    "mileage_on_vehicle": "vehicles.mileage.value",
                    "deal_type": "type",
                    "cost_of_vehicle": "vehicles.pricing.finalCost.amount",
                    "oem_msrp": "vehicles.pricing.msrp.amount",
                    "adjustment_on_price": "vehicles.pricing.totalAdjustments.amount",
                    "payoff_on_trade": "tradeIns.tradePayOff.amount",
                    "miles_per_year": "dealPayment.termPayment.yearlyMiles.totalValue",
                    "profit_on_sale": "vehicles.pricing.profit.amount",
                    "has_service_contract": "dealPayment.fnIs.disclosureType",
                    "vehicle_gross": "vehicles.pricing.retailPrice.amount",
                    "vin": "vehicles.vin",
                    "delivery_date": "deliveryDate",
                    "deal_payment": "dealPayment",
                    "finance_amount": "dealPayment.termPayment.amountFinanced.amount",
                    "finance_rate": "dealPayment.termPayment.apr.apr"
                },
            },
            "tekioncrawlerdb_repair_order": {
                "dealer_integration_partner": {"dms_id": "dms_id"},
                "consumer": {
                    "first_name": "customer.firstName",
                    "last_name": "customer.lastName",
                    "email": "customer.email",
                    "phones": "customer.phones",
                    "cell_phone": "customer.phones",
                    "home_phone": "customer.phones",
                    "city": "customer.address.city",
                    "state": "customer.address.state",
                    "postal_code": "customer.address.zip"
                },
                "vehicle": {
                    "vehicles": "vehicles",
                    "vin": "vehicle.vin",
                    "make": "vehicle.make",
                    "model": "vehicle.model",
                    "year": "vehicle.year"
                },
                "service_repair_order": {
                    "ro_open_date": "createdTime",
                    "ro_close_date": "closedTime",
                    "txn_pay_type": "jobs.payType",
                    "repair_order_no": "repairOrderNumber",
                    "advisor_name": "primaryAdvisor",
                    "advisor_first_name": "primaryAdvisor.firstName",
                    "advisor_last_name": "primaryAdvisor.lastName",
                    "total_amount": "invoice.invoiceAmount",
                    "consumer_total_amount": "invoice.customerPay.amount",
                    "warranty_total_amount": "invoice.warrantyPay.amount",
                    "internal_total_amount": "invoice.internalPay.amount",
                    "comment": "jobs.concern"
                }
            }
        }

    def select_columns(self, df, table_to_mappings):
        """Select valid db columns from a dataframe using dms column mappings, log and skip missing data."""
        ignore_columns = []
        selected_columns = []
        for db_table_name, db_columns_to_dms_columns in table_to_mappings.items():
            for db_column, dms_column in db_columns_to_dms_columns.items():
                try:
                    df.select(dms_column)
                except pyspark.sql.utils.AnalysisException:
                    logger.warning(
                        f"Column: {db_column} with mapping: {dms_column} not found, default to null."
                    )
                    ignore_columns.append(db_column)
                else:
                    selected_columns.append(F.col(dms_column).alias(f"{db_table_name}|{db_column}"))

            if db_table_name in self.metadata_tables:
                metadata_column = f"{db_table_name}|metadata"
                df = df.withColumn(
                    metadata_column,
                    F.to_json(
                        F.struct(
                            F.lit(self.region).alias("Region"),
                            "PartitionYear",
                            "PartitionMonth",
                            "PartitionDate",
                            F.col("id"),
                            F.input_file_name().alias("s3_url"),
                        )
                    ),
                )
                selected_columns.append(metadata_column)

        selected_columns += ["PartitionYear", "PartitionMonth", "PartitionDate"]
        logging.warning(f"Selected columns: {selected_columns}")
        return df.select(selected_columns)

    def apply_mappings(self, df, catalog_name):
        """Map the raw data to the columns and return as a dataframe."""
        if catalog_name not in ("tekioncrawlerdb_fi_closed_deal", "tekioncrawlerdb_repair_order"):
            raise RuntimeError(f"Unexpected catalog {catalog_name}")

        valid_data = df.select(
            "PartitionYear",
            "PartitionMonth",
            "PartitionDate",
            "*",
        )

        # Select columns raw data by mapping
        table_data = self.select_columns(valid_data, self.mappings[catalog_name])
        return table_data

    def extract_first_item_from_array(self, df, column_name):
        df = df.withColumn(column_name, F.when(F.size(column_name) > 0, F.col(column_name).getItem(0)).otherwise(None))
        return df

    def extract_first_item_from_nested_array(self, df, column_name):
        df = df.withColumn(column_name, F.when(F.size(F.col(column_name)) > 0, F.col(column_name).getItem(0).getItem(0)).otherwise(None))
        return df

    def filter_phone_type(self, df, field_name, column_name, phone_type):
        phone = F.filter(F.col("consumer|phones"), lambda x: x[field_name] == phone_type)[0]["number"]
        df = df.withColumn(column_name, phone.cast(StringType()))
        return df

    def set_optin_flag(self, df, flag_column, communication_type):
        if flag_column in df.columns:
            df = df.withColumn(flag_column,
                F.when((F.col(flag_column)["isOptInService"][0] == True) |
                    (F.col(flag_column)["isOptInMarketing"][0] == True),
                    True).otherwise(False))
        return df

    def convert_unix_to_timestamp(self, df, column_name):
        df = df.withColumn(
            column_name,
            F.when(
                (F.col(column_name) <= 0) | (F.col(column_name).isNull()),
                None
            ).otherwise(
                F.from_unixtime(
                    F.col(column_name).cast("double") / 1000,
                    'yyyy-MM-dd HH:mm:ss'
                ).cast("timestamp")
            )
        )
        return df

    def format_df(self, df, catalog_name):
        """Format the raw data to match the database schema."""
        if catalog_name == "tekioncrawlerdb_fi_closed_deal":
            if "consumer|customers" in df.columns:
                df = self.extract_first_item_from_array(df, "consumer|customers")
                df = self.extract_first_item_from_array(df, "consumer|first_name")
                df = self.extract_first_item_from_array(df, "consumer|last_name")
                df = self.extract_first_item_from_nested_array(df, "consumer|email")
                df = df.withColumn("consumer|email", (F.col("consumer|email")["emailId"]))
            if "consumer|phones" in df.columns:
                df = self.extract_first_item_from_array(df, "consumer|phones")
                df = self.filter_phone_type(df, 'type', 'consumer|home_phone', 'HOME')
                df = self.filter_phone_type(df, 'type', 'consumer|cell_phone', 'CELL')
                df = df.drop("consumer|phones")
            if "consumer|addresses" in df.columns:
                df = self.extract_first_item_from_nested_array(df, "consumer|addresses")
                df = df.withColumn('consumer|city', F.col("consumer|addresses")["city"])
                df = df.withColumn('consumer|state', F.col("consumer|addresses")["state"])
                df = df.withColumn('consumer|postal_code', F.col("consumer|addresses")["zip"])
                df = df.drop("consumer|addresses")
            if "consumer|email_optin_flag" in df.columns:
                df = self.set_optin_flag(df, "consumer|email_optin_flag", "email")
            if "consumer|phone_optin_flag" in df.columns:
                df = self.set_optin_flag(df, "consumer|phone_optin_flag", "call")
            if "consumer|sms_optin_flag" in df.columns:
                df = self.set_optin_flag(df, "consumer|sms_optin_flag", "text")
            if "consumer|postal_mail_optin_flag" in df.columns:
                df = self.set_optin_flag(df, "consumer|postal_mail_optin_flag", "mail")
            if "vehicle|vehicles" in df.columns:
                columns_to_extract_first_item = ["vehicle|vehicles", "vehicle|vin", "vehicle|make", "vehicle|model", "vehicle|year", "vehicle|vehicle_class"]
                for column_name in columns_to_extract_first_item:
                    df = self.extract_first_item_from_array(df, column_name)
                df = df.withColumn("vehicle|mileage", F.col("vehicle|mileage")[0].cast('int'))
                df = df.withColumn("vehicle|new_or_used", F.when(F.col("vehicle|new_or_used")[0] == "NEW", "N").otherwise("U"))
                df = df.withColumn("vehicle|type", F.col("vehicle|new_or_used"))
            if "vehicle_sale|sale_date" in df.columns:
                columns_to_extract_first_item = ["vehicle_sale|listed_price", "vehicle_sale|mileage_on_vehicle", "vehicle_sale|oem_msrp",
                                                 "vehicle_sale|payoff_on_trade", "vehicle_sale|profit_on_sale",
                                                 "vehicle_sale|adjustment_on_price", "vehicle_sale|vehicle_gross", "vehicle_sale|cost_of_vehicle", "vehicle_sale|vin"]
                for column_name in columns_to_extract_first_item:
                    df = self.extract_first_item_from_array(df, column_name)
                df = df.withColumn("vehicle_sale|mileage_on_vehicle", F.col("vehicle_sale|mileage_on_vehicle").cast('int'))
                df = df.withColumn("vehicle_sale|has_service_contract", F.array_contains(F.col('vehicle_sale|has_service_contract'), 'SERVICE_CONTRACT'))
                df = self.convert_unix_to_timestamp(df, "vehicle_sale|sale_date")
                if "vehicle_sale|delivery_date" in df.columns:
                    df = self.convert_unix_to_timestamp(df, "vehicle_sale|delivery_date")
            df = df.drop("consumer|customers", "vehicle|vehicles", "vehicle_sale|deal_payment")

        if catalog_name == "tekioncrawlerdb_repair_order":
            if "consumer|phones" in df.columns:
                df = self.filter_phone_type(df, 'phoneType', 'consumer|home_phone', 'HOME')
                df = self.filter_phone_type(df, 'phoneType', 'consumer|cell_phone', 'MOBILE')
                df = df.drop("consumer|phones")
            if "service_repair_order|ro_open_date" in df.columns:
                df = self.convert_unix_to_timestamp(df, "service_repair_order|ro_open_date")
                df = self.convert_unix_to_timestamp(df, "service_repair_order|ro_close_date")
                df = df.withColumn("service_repair_order|txn_pay_type", F.concat_ws(", ", F.array_distinct(F.col("service_repair_order|txn_pay_type"))))
                df = df.withColumn("service_repair_order|advisor_name", F.concat(F.col("service_repair_order|advisor_first_name")[0], F.lit(" "), F.col("service_repair_order|advisor_last_name")[0]))
                df = df.withColumn("service_repair_order|comment", F.concat_ws(", ", F.array_distinct(F.col("service_repair_order|comment"))))

            df = df.drop("service_repair_order|advisor_first_name", "service_repair_order|advisor_last_name")
        return df

    def add_list_to_df(
        self, df, add_list, add_list_column_name, temp_col_name="order_temp"
    ):
        """Given a dataframe and a list, add the list as a column to the dataframe preserving order."""
        w = Window.partitionBy(F.lit(1)).orderBy(F.lit(1))
        temp_df = self.spark.createDataFrame(
            [[x] for x in add_list], [add_list_column_name]
        ).withColumn(temp_col_name, F.row_number().over(w))
        df = (
            df.withColumn(temp_col_name, F.row_number().over(w))
            .join(temp_df, [temp_col_name])
            .drop(F.col(temp_col_name))
        )
        return df

    def save_df_notify(self, df, s3_key):
        """Save schema and data to s3, notify of error."""
        schema_json = loads(df.schema.json())
        data_json = [row.asDict(recursive=True) for row in df.collect()]
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=dumps({"schema": schema_json, "data": data_json}),
            ContentType="application/json",
        )
        logger.info(f"Uploaded df info to {s3_key}")
        sqs_client = boto3.client("sqs")
        sqs_client.send_message(
            QueueUrl=self.dlq_url,
            MessageBody=dumps({"bucket": self.bucket_name, "key": s3_key}),
        )

    def validate_fields(self, df):
        """ Check that each df column names matches the database. """
        # Ignore op codes (many to many array relationship) and partition columns
        ignore_table_names = ["service_contracts", "op_codes", "PartitionYear", "PartitionMonth", "PartitionDate"]
        unified_column_names = self.rds.get_unified_column_names()
        for df_col in df.columns:
            if df_col.split("|")[0] not in ignore_table_names and df_col not in unified_column_names:
                raise RuntimeError(f"Column {df_col} not found in database {unified_column_names}")

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            if "tekioncrawlerdb_fi_closed_deal" == catalog_name:
                s3_key_path = "fi_closed_deal"
                main_column_name = "dealNumber"
            elif "tekioncrawlerdb_repair_order" == catalog_name:
                s3_key_path = "repair_order"
                main_column_name = "repairOrderNumber"
            else:
                raise RuntimeError(f"Unexpected catalog {catalog_name}")

            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=f"context_{catalog_name}",
            )

            if (
                datasource.filter(lambda row: row[main_column_name] is not None).count()
                == 0
            ):
                logger.info("No new data to parse")
                return

            # Rename partitions from s3
            datasource = (
                datasource.withColumnRenamed("partition_0", "PartitionYear")
                .withColumnRenamed("partition_1", "PartitionMonth")
                .withColumnRenamed("partition_2", "PartitionDate")
            )

            # Resolve choices, flatten dataframe
            datasource = self.resolver.resolve_choices(
                datasource, main_column_name
            ).toDF()

            datasource.printSchema()

            # Get the base fields in a df via self.mappings
            datasource = self.apply_mappings(datasource, catalog_name)

            # Format necessary base fields to get a standardized df format
            datasource = self.format_df(datasource, catalog_name)

            # Validate dataframe fields meet standardized df format
            self.validate_fields(datasource)

            # Convert the DataFrame back to a DynamicFrame (testing)
            dynamic_frame = DynamicFrame.fromDF(datasource, self.glue_context, "dynamic_frame")

            # Write out the result dataset to S3 (testing)
            self.glue_context.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", connection_options={"path": "s3://integrations-etl-test/tekion/results/"}, format="json")

            # Write to S3
            s3_path = f"s3a://{self.bucket_name}/unified/{s3_key_path}/tekion/"

            datasource.write.partitionBy(
                "dealer_integration_partner|dms_id",
                "PartitionYear",
                "PartitionMonth",
                "PartitionDate"
            ).mode(
                "append"
            ).parquet(
                s3_path
            )

            logger.info(f"Uploaded to {s3_path}")

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
            "region",
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
