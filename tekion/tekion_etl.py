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
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType
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

    def select_db_dealer_integration_partner_id(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_integration_partner_id_query = f"""
            select dip.id from {self.schema}."dealer_integration_partner" dip
            join {self.schema}."integration_partner" i on dip.integration_partner_id = i.id 
            where dip.dms_id = '{dms_id}' and i.impel_integration_partner_id = '{self.integration}' and dip.is_active = true;"""
        results = self.execute_rds(db_dealer_integration_partner_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No active dealer {dms_id} found with query {db_dealer_integration_partner_id_query}."
            )
        else:
            return results[0]
    
    def insert_service_repair_order(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into service repair order table and return row count."""
        desired_service_repair_order_columns = mappings[catalog_name][
            "service_repair_order"
        ].keys()
        actual_service_repair_order_columns = [
            x for x in desired_service_repair_order_columns if x in dealer_df.columns
        ]
        actual_service_repair_order_columns.append("dealer_integration_partner_id")
        actual_service_repair_order_columns.append("consumer_id")
        actual_service_repair_order_columns.append("vehicle_id")

        unique_ros_dms_cols = ["repair_order_no", "dealer_integration_partner_id"]
        service_repair_order_df = dealer_df.select(
            actual_service_repair_order_columns
        ).dropDuplicates(subset=unique_ros_dms_cols)
        insert_service_repair_order_query = self.get_insert_query_from_df(
            service_repair_order_df,
            "service_repair_order",
            f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
            SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, service_repair_order.{x})' for x in service_repair_order_df.columns])}
            RETURNING id""",
        )

        results = self.commit_rds(insert_service_repair_order_query)

        if results is None:
            return []
        inserted_service_repair_order_ids = [x[0] for x in results.fetchall()]
        return inserted_service_repair_order_ids

    def insert_vehicle_sale(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into vehicle sale table and return row count."""
        desired_vehicle_sale_columns = mappings[catalog_name]["vehicle_sale"].keys()
        actual_vehicle_sale_columns = [
            x for x in desired_vehicle_sale_columns if x in dealer_df.columns
        ]
        actual_vehicle_sale_columns.append("dealer_integration_partner_id")
        actual_vehicle_sale_columns.append("consumer_id")
        actual_vehicle_sale_columns.append("vehicle_id")

        dealer_df.show()
        dealer_df.select("days_in_stock").show()
        dealer_df.select("cost_of_vehicle").show()
        dealer_df.printSchema()

        vehicle_sale_df = dealer_df.select(actual_vehicle_sale_columns)
        insert_vehicle_sale_query = self.get_insert_query_from_df(
            vehicle_sale_df,
            "vehicle_sale",
            "ON CONFLICT ON CONSTRAINT unique_vehicle_sale DO NOTHING RETURNING id",
        )

        results = self.commit_rds(insert_vehicle_sale_query)

        if results is None:
            return []
        inserted_vehicle_sale_ids = [x[0] for x in results.fetchall()]
        return inserted_vehicle_sale_ids

    def insert_consumer(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into consumer table and return created ids."""
        desired_consumer_columns = mappings[catalog_name]["consumer"].keys()
        actual_consumer_columns = [
            x for x in desired_consumer_columns if x in dealer_df.columns
        ]
        actual_consumer_columns.append("dealer_integration_partner_id")

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


    def insert_vehicle(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into vehicle table and return created ids."""
        desired_vehicle_columns = mappings[catalog_name]["vehicle"].keys()
        actual_vehicle_columns = [
            x for x in desired_vehicle_columns if x in dealer_df.columns
        ]
        actual_vehicle_columns.append("dealer_integration_partner_id")

        vehicle_df = dealer_df.select(actual_vehicle_columns)
        insert_vehicle_query = self.get_insert_query_from_df(
            vehicle_df, "vehicle", "RETURNING id"
        )

        results = self.commit_rds(insert_vehicle_query)
        if results is None:
            return []
        inserted_vehicle_ids = [x[0] for x in results.fetchall()]
        return inserted_vehicle_ids

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
        self.bucket_name = (
            f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        )
        self.rds = RDSInstance(self.is_prod, self.integration)
        self.mappings = {
            "tekioncrawlerdb_deal": {
                # TODO: get dealer_integration_partner_id from the onboarding team
                "dealer": {"dms_id": "techmotors_4"},
                "consumer": {
                    "customers": "data.customers",
                    "first_name": "data.customers.firstName",
                    "last_name": "data.customers.lastName",
                    "email": "data.customers.emails",
                    "phones": "data.customers.phones",
                    "cell_phone": "data.customers.phones",
                    "home_phone": "data.customers.phones",
                    "addresses": "data.customers.addresses",
                    "city": "data.customers.addresses",
                    "state": "data.customers.addresses",
                    "postal_code": "data.customers.addresses",
                    "email_optin_flag": "data.customers.communicationPreferences.email",
                    "phone_optin_flag": "data.customers.communicationPreferences.call",
                    "postal_mail_optin_flag": "data.customers.communicationPreferences.mail",
                    "sms_optin_flag": "data.customers.communicationPreferences.text"
                },
                "vehicle": {
                    "vehicles": "data.vehicles",
                    "vin": "data.vehicles.vin",
                    "type": "data.vehicles.type",
                    "vehicle_class": "data.vehicles.trimDetails.bodyClass",
                    "mileage": "data.vehicles.mileage.value",
                    "make": "data.vehicles.make",
                    "model": "data.vehicles.model",
                    "year": "data.vehicles.year",
                    "new_or_used": "data.vehicles.stockType"
                },
                "vehicle_sale": {
                    "created_date": "data.createdTime",
                    "days_in_stock": "",
                    "sale_date": "data.contractDate",
                    "listed_price": "data.vehicles.pricing.retailPrice.amount",
                    "sales_tax": "data.dealPayment.termPayment.totals.taxAmount.amount",
                    "mileage_on_vehicle": "data.vehicles.mileage.value",
                    "deal_type": "data.type",
                    "cost_of_vehicle": "data.vehicles.pricing.finalCost.amount",
                    "oem_msrp": "data.vehicles.pricing.msrp.amount",
                    "adjustment_on_price": "data.vehicles.pricing.totalAdjustments.amount",
                    "payoff_on_trade": "data.tradeIns.tradePayOff.amount",
                    "miles_per_year": "data.dealPayment.termPayment.yearlyMiles.totalValue",
                    "profit_on_sale": "data.vehicles.pricing.profit.amount",
                    "has_service_contract": "data.dealPayment.fnIs.disclosureType.ServiceContract",
                    "vehicle_gross": "data.vehicles.pricing.retailPrice.amount",
                    "vin": "deal.vehicles.vin",
                    "delivery_date": "data.deliveryDate",
                    "deal_payment": "data.dealPayment",
                    "finance_amount": "data.dealPayment.termPayment.amountFinanced.amount",
                    "finance_rate": "data.dealPayment.termPayment"
                },
            },
            "tekioncrawlerdb_repair_order": {
                # TODO: get dealer_integration_partner_id from the onboarding team
                "dealer": {"dms_id": "techmotors_4"},
                "consumer": {
                    "first_name": "data.customer.firstName",
                    "last_name": "data.customer.lastName",
                    "email": "data.customer.email",
                    "phones": "data.customer.phones",
                    "cell_phone": "data.customer.phones",
                    "home_phone": "data.customer.phones",
                    "city": "data.customer.addresses.city",
                    "state": "data.customer.addresses.state",
                    "postal_code": "data.customer.addresses.zip"
                },
                "vehicle": {
                    "vehicles": "data.vehicles",
                    "vin": "data.vehicle.vin",
                    "make": "data.vehicle.make",
                    "model": "data.vehicle.model",
                    "year": "data.vehicle.year"
                },
                "service_repair_order": {
                    "ro_open_date": "data.createdTime",
                    "ro_close_date": "data.closedTime",
                    "txn_pay_type": "data.jobs.payType",
                    "repair_order_no": "data.repairOrderNumber",
                    "advisor_name": "data.primaryAdvisor",
                    "advisor_first_name": "data.primaryAdvisor.firstName",
                    "advisor_last_name": "data.primaryAdvisor.lastName",
                    "total_amount": "data.invoice.invoiceAmount",
                    "consumer_total_amount": "data.invoice.customerPay.amount",
                    "warranty_total_amount": "data.invoice.warrantyPay.amount",
                    "comment": "data.jobs.concern"
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
        if catalog_name in ("tekioncrawlerdb_deal", "tekioncrawlerdb_repair_order"):
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
        if catalog_name == "tekioncrawlerdb_deal":
            if "customers" in df.columns:
                df = df.withColumn("first_name", F.col("customers")[0]["firstName"])
                df = df.withColumn("last_name", F.col("customers")[0]["lastName"])
                df = df.withColumn("email", F.col("customers")[0]["emails"][0]["emailId"])
            if "phones" in df.columns:
                df = df.withColumn('phones', F.col("customers")[0]["phones"])
                df = df.withColumn('home_phone', F.expr("filter(phones, x -> x.type = 'HOME')[0].number"))
                df = df.withColumn('cell_phone', F.expr("filter(phones, x -> x.type = 'CELL')[0].number"))
                df = df.drop("phones")
            if "addresses" in df.columns:
                df = df.withColumn('city', F.col('customers').getItem(0).getField('addresses').getItem(0).getField('city'))
                df = df.withColumn('state', F.col('customers').getItem(0).getField('addresses').getItem(0).getField('state'))
                df = df.withColumn('postal_code', F.col('customers').getItem(0).getField('addresses').getItem(0).getField('zip'))
                df = df.drop("addresses")
            if "email_optin_flag" in df.columns:
                df = df.withColumn("email_optin_flag", 
                        F.when((F.col("customers")[0]["communicationPreferences"]["email"]["isOptInService"] == True) |
                            (F.col("customers")[0]["communicationPreferences"]["email"]["isOptInMarketing"] == True),
                            True).otherwise(False))
            if "phone_optin_flag" in df.columns:
                df = df.withColumn("phone_optin_flag", 
                    F.when((F.col("customers")[0]["communicationPreferences"]["call"]["isOptInService"] == True) |
                            (F.col("customers")[0]["communicationPreferences"]["call"]["isOptInMarketing"] == True),
                            True).otherwise(False))
            if "sms_optin_flag" in df.columns:
                df = df.withColumn("sms_optin_flag", 
                    F.when((F.col("customers")[0]["communicationPreferences"]["text"]["isOptInService"] == True) |
                            (F.col("customers")[0]["communicationPreferences"]["text"]["isOptInMarketing"] == True),
                            True).otherwise(False))
            if "postal_mail_optin_flag" in df.columns:
                df = df.withColumn("postal_mail_optin_flag", 
                    F.when((F.col("customers")[0]["communicationPreferences"]["mail"]["isOptInService"] == True) |
                            (F.col("customers")[0]["communicationPreferences"]["mail"]["isOptInMarketing"] == True),
                            True).otherwise(False))
            if "vehicles" in df.columns:
                df = df.withColumn("vin", F.col("vehicles")[0]["vin"])
                df = df.withColumn("make", F.col("vehicles")[0]["make"])
                df = df.withColumn("type", F.col("vehicles")[0]["stockType"])
                df = df.withColumn("mileage", F.col("vehicles")[0]["mileage"]["value"])
                df = df.withColumn("model", F.col("vehicles")[0]["model"])
                df = df.withColumn("year", F.col("vehicles")[0]["year"])
                df = df.withColumn("new_or_used", F.when(F.col("vehicles")[0]["stockType"] == "NEW", "N").otherwise("U"))
                df = df.withColumn("vehicle_class", F.col("vehicles")[0]["trimDetails"]["bodyClass"])
            if "sale_date" in df.columns:
                df = df.withColumn("sale_date", F.from_unixtime(F.col("sale_date").getField("long") / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                df = df.withColumn("sales_tax", F.col("sales_tax")["double"])
                df = df.withColumn("listed_price", F.col("listed_price")[0])
                df = df.withColumn("mileage_on_vehicle", F.col("mileage_on_vehicle")[0])
                df = df.withColumn("cost_of_vehicle", F.coalesce(
                    F.col("cost_of_vehicle")["double"][0],
                    F.col("cost_of_vehicle")["int"][0].cast(DoubleType())
                ))
                df = df.withColumn("oem_msrp", F.col("oem_msrp")[0])
                df = df.withColumn("adjustment_on_price", F.col("adjustment_on_price")[0])
                df = df.withColumn("vin", F.col("vehicles")[0]["vin"])
                
                df = df.withColumn("sold_date", F.from_unixtime(F.col("vehicles")[0]["soldTime"]["long"] / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                df = df.withColumn("created_date", F.from_unixtime(F.col("created_date") / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                
                df = df.withColumn("days_in_stock",
                    F.when(F.col("sold_date").isNull() | F.col("created_date").isNull(), None)
                    .otherwise(F.abs(F.datediff(F.col("sold_date"), F.col("created_date")))))
                
                df = df.withColumn("payoff_on_trade", F.col("payoff_on_trade")[0])
                df = df.withColumn("profit_on_sale", F.col("profit_on_sale")[0])
                df = df.withColumn("has_service_contract", F.expr("array_contains(deal_payment.fnIs.disclosureType, 'SERVICE_CONTRACT')"))
                df = df.withColumn("vehicle_gross", F.col("vehicle_gross")[0])
                df = df.withColumn("delivery_date", F.from_unixtime(F.col("delivery_date").getField("long") / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                df = df.withColumn("finance_rate", F.coalesce(
                    F.col("deal_payment")["termPayment"]["apr"]["apr"]["double"].cast(StringType()),
                    F.col("deal_payment")["termPayment"]["apr"]["apr"]["int"].cast(StringType())
                ))

            df = df.drop("vehicles", "customers", "deal_payment", "sold_date", "created_date")

        if catalog_name == "tekioncrawlerdb_repair_order":
            if "phones" in df.columns:
                df = df.withColumn('home_phone', F.expr("filter(phones, x -> x.phoneType = 'HOME')[0].number"))
                df = df.withColumn('cell_phone', F.expr("filter(phones, x -> x.phoneType = 'MOBILE')[0].number"))
                df = df.drop("phones")
            if "ro_open_date" in df.columns:
                df = df.withColumn("ro_open_date", F.from_unixtime(F.col("ro_open_date") / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                df = df.withColumn("ro_close_date", F.from_unixtime(F.col("ro_close_date") / 1000, 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
                df = df.withColumn("txn_pay_type", F.concat_ws(", ", F.array_distinct(F.col("txn_pay_type"))))
                df = df.withColumn("advisor_name", F.concat(F.col("advisor_first_name")[0], F.lit(" "), F.col("advisor_last_name")[0]))
                df = df.withColumn("comment", F.concat_ws(", ", F.array_distinct(F.col("comment"))))

            df = df.drop("advisor_first_name", "advisor_last_name")

        if starting_count != df.count():
            raise RuntimeError(
                f"Error formatting lost data from {starting_count} rows to {df.count()}"
            )
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
    
    def upsert_df(self, df, catalog_name):
        """Upsert dataframe to RDS table."""
        insert_count = 0
        current_dealer = None
        dealers = df.select("dms_id").distinct().collect()
        if len(dealers) == 0:
            logger.error("No data found for any dealer")
        for dealer in dealers:
            try:
                current_dealer = dealer.dms_id
                dealer_df = df.filter(df.dms_id == dealer.dms_id)
                db_dealer_integration_partner_id = None

                db_dealer_integration_partner_id = (
                    self.rds.select_db_dealer_integration_partner_id(dealer.dms_id)
                )
                dealer_df = dealer_df.withColumn(
                    "dealer_integration_partner_id",
                    F.lit(db_dealer_integration_partner_id),
                )
                dealer_rows = dealer_df.count()
                if catalog_name == "tekioncrawlerdb_deal":
                    # Vehicle sale must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    if count != dealer_rows:
                        raise RuntimeError(f"Unable to insert consumers, expected {dealer_rows} got {count}")
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )
                    vehicle_sale_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    # Then insert into vehicle
                    inserted_vehicle_ids = self.rds.insert_vehicle(
                        vehicle_sale_df, catalog_name, self.mappings
                    )
                    count = len(inserted_vehicle_ids)
                    if count != dealer_rows:
                        raise RuntimeError(f"Unable to insert vehicles, expected {dealer_rows} got {count}")
                    logger.info(
                        f"Added {count} rows to vehicle for dealer {db_dealer_integration_partner_id}"
                    )
                    vehicle_sale_df = self.add_list_to_df(
                        vehicle_sale_df, inserted_vehicle_ids, "vehicle_id"
                    )

                    # Then insert into vehicle sale
                    vehicle_sale_ids = self.rds.insert_vehicle_sale(
                        vehicle_sale_df, catalog_name, self.mappings
                    )
                    count = len(vehicle_sale_ids)
                    logger.info(
                        f"Added {count} rows to vehicle_sale for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

                elif catalog_name == "tekioncrawlerdb_repair_order":
                    # Service repair order must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )
                    service_repair_order_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    # Then insert into vehicle
                    inserted_vehicle_ids = self.rds.insert_vehicle(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(inserted_vehicle_ids)
                    if count != dealer_rows:
                        raise RuntimeError(f"Unable to insert vehicles, expected {dealer_rows} got {count}")
                    logger.info(
                        f"Added {count} rows to vehicle for dealer {db_dealer_integration_partner_id}"
                    )
                    service_repair_order_df = self.add_list_to_df(
                        service_repair_order_df, inserted_vehicle_ids, "vehicle_id"
                    )

                    # Then insert into service repair orders
                    service_repair_order_ids = self.rds.insert_service_repair_order(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(service_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to service_repair_order for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

            except Exception:
                s3_key = f"{self.integration}/errors/{datetime.now().strftime('%Y-%m-%d')}/{self.job_id}/{uuid.uuid4().hex}.json"
                logger.exception(f"Error inserting {catalog_name} for dealer {dealer_df} save data {s3_key}")
                self.save_df_notify(dealer_df, s3_key)
        return insert_count
    
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

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            if "tekioncrawlerdb_deal" == catalog_name:
                main_column_name = "Deal"
            elif "tekioncrawlerdb_repair_order" == catalog_name:
                main_column_name = "RepairOrder"
            else:
                raise RuntimeError(f"Unexpected catalog {catalog_name}")

            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=f"context_{catalog_name}",
            )

            if datasource.filter(lambda row: row[main_column_name] is not None).count() == 0:
                logger.info("No new data to parse")
                return

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
            
            # Convert the DataFrame back to a DynamicFrame (testing)
            dynamic_frame = DynamicFrame.fromDF(formatted_df, self.glue_context, "dynamic_frame")
            
            # Write out the result dataset to S3 (testing)
            self.glue_context.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", connection_options={"path": self.bucket_name}, format="json")

            # Insert tables to database
            upsert_count = self.upsert_df(formatted_df, catalog_name)
            
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
