"""Rey Rey ETL Job."""
import logging
import sys
import uuid
from json import dumps, loads

import boto3
import psycopg2
import pyspark
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self, is_prod, schema, integration):
        self.integration = integration
        self.is_prod = is_prod
        self.schema = schema
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
            values_str = f"({''.join('%s, ' for _ in range(len(records[0])))[:-2]})"
            args_str = ",".join(
                cursor.mogrify(values_str, x).decode("utf-8") for x in records
            )
            columns_str = ",".join(columns)
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


class ReyReyUpsertJob:
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
        self.integration = "reyrey"
        self.is_prod = args["environment"] == "prod"
        self.schema = f"{'prod' if self.is_prod else 'stage'}"
        self.bucket_name = (
            f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        )
        self.rds = RDSInstance(self.is_prod, self.schema, self.integration)
        self.mappings = {
            "reyreycrawlerdb_fi_closed_deal": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "consumer": {
                    "dealer_customer_no": "FIDeal.Buyer.CustRecord.ContactInfo._NameRecId",
                    "first_name": "FIDeal.Buyer.CustRecord.ContactInfo._FirstName",
                    "last_name": "FIDeal.Buyer.CustRecord.ContactInfo._LastName",
                    "email": "FIDeal.Buyer.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone.Array",
                    "city": "FIDeal.Buyer.CustRecord.ContactInfo.Address._City",
                    "state": "FIDeal.Buyer.CustRecord.ContactInfo.Address._State",
                    "postal_code": "FIDeal.Buyer.CustRecord.ContactInfo.Address._Zip",
                    "home_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone.Array",
                },
                "vehicle_sale": {
                    "sale_date": "FIDeal.FIDealFin._CloseDealDate",
                    "listed_price": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    "sales_tax": "FIDeal.FIDealFin.FinanceInfo.TaxAmounts.TotTaxes",
                    "mileage_on_vehicle": "FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._OdomReading",
                    "deal_type": "FIDeal.FIDealFin._Category",
                    "cost_of_vehicle": "FIDeal.FIDealFin.TransactionVehicle._VehCost",
                    "oem_msrp": "FIDeal.FIDealFin.TransactionVehicle._MSRP",
                    "adjustment_on_price": "FIDeal.FIDealFin.TransactionVehicle._Discount",
                    "days_in_stock": "FIDeal.FIDealFin.TransactionVehicle._DaysInStock",
                    "date_of_state_inspection": "FIDeal.FIDealFin.TransactionVehicle._InspectionDate",
                    "new_or_used": "FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._NewUsed",
                    "trade_in_value": "FIDeal.FIDealFin.TradeIn._ActualCashValue",
                    "payoff_on_trade": "FIDeal.FIDealFin.TradeIn._Payoff",
                    "value_at_end_of_lease": "FIDeal.FIDealFin.FinanceInfo.LeaseSpec._VehicleResidual",
                    "miles_per_year": "FIDeal.FIDealFin.FinanceInfo.LeaseSpec._EstDrvYear",
                    "profit_on_sale": "FIDeal.FIDealFin.Recap.Reserves._NetProfit",
                    "has_service_contract": "FIDeal.FIDealFin.WarrantyInfo.ServiceCont._ServContYN",
                    "vehicle_gross": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    "warranty_expiration_date": "FIDeal.FIDealFin.WarrantyInfo.ExtWarranty.VehExtWarranty._ExpirationDate",
                    "service_package_flag": "FIDeal.FIDealFin.WarrantyInfo.ServiceCont.CompanyName",
                    "vin": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin",
                    "make": "FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._VehicleMake",
                    "model": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._ModelDesc",
                    "year": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr",
                    "delivery_date": "FIDeal.FIDealFin._DeliveryDate",
                },
            },
            "reyreycrawlerdb_repair_order": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "op_codes": {
                    "repair_order_no": "RepairOrder.RoRecord.Rogen._RoNo",
                    "opcodes": "RepairOrder.RoRecord.Rogen.RecommendedServc",
                },
                "consumer": {
                    "dealer_customer_no": "RepairOrder.CustRecord.ContactInfo._NameRecId",
                    "first_name": "RepairOrder.CustRecord.ContactInfo._FirstName",
                    "last_name": "RepairOrder.CustRecord.ContactInfo._LastName",
                    "email": "RepairOrder.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "RepairOrder.CustRecord.ContactInfo.phone.Array",
                    "city": "RepairOrder.CustRecord.ContactInfo.Address._City,",
                    "state": "RepairOrder.CustRecord.ContactInfo.Address._State",
                    "postal_code": "RepairOrder.CustRecord.ContactInfo.Address._Zip",
                    "home_phone": "RepairOrder.CustRecord.ContactInfo.phone.Array",
                },
                "service_repair_order": {
                    "ro_open_date": "RepairOrder.RoRecord.Rogen._RoCreateDate",
                    "ro_close_date": "RepairOrder.ServVehicle.VehicleServInfo._LastRODate",
                    "txn_pay_type": "RepairOrder.RoRecord.Rolabor.RoAmts._PayType",
                    "repair_order_no": "RepairOrder.RoRecord.Rogen._RoNo",
                    "advisor_name": "RepairOrder.RoRecord.Rogen._AdvName",
                    "total_amount": "RepairOrder.RoRecord.Rolabor.RoAmts._TxblAmt",
                    "consumer_total_amount": "RepairOrder.RoRecord.Rogen._CustRoTotalAmt",
                    "warranty_total_amount": "RepairOrder.RoRecord.Rogen._WarrRoTotalAmt",
                    "comment": "RepairOrder.RoRecord.Rogen.RoCommentInfo",
                    "recommendation": "RepairOrder.RoRecord.Rogen.TechRecommends._TechRecommend",
                },
            },
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
        return df.select(selected_columns)

    def apply_mappings(self, df, catalog_name):
        """Map the raw data to the unified column and return as a dataframe."""
        if "reyreycrawlerdb_fi_closed_deal" == catalog_name:
            data_column_name = "FIDeal"
        elif "reyreycrawlerdb_repair_order" == catalog_name:
            data_column_name = "RepairOrder"
        else:
            raise RuntimeError(f"Unexpected catalog {catalog_name}")

        # Log data with null values
        null_data = df.where(F.col(f"{data_column_name}.Array").isNull()).select(
            "Year",
            "Month",
            "Date",
            "ApplicationArea.Sender.DealerNumber",
            "ApplicationArea.BODId",
        )
        null_data_json = null_data.toJSON().collect()
        logging.warning(f"Skip processing null data: {null_data_json}")

        # Log and select data without null values
        valid_data = df.where(F.col(f"{data_column_name}.Array").isNotNull()).select(
            "Year",
            "Month",
            "Date",
            "ApplicationArea",
            F.explode(f"{data_column_name}.Array").alias(data_column_name),
        )
        valid_data_json = (
            valid_data.select(
                "Year",
                "Month",
                "Date",
                "ApplicationArea.Sender.DealerNumber",
                "ApplicationArea.BODId",
            )
            .toJSON()
            .collect()
        )
        logging.info(f"Processing data: {valid_data_json}")

        # Select columns raw data by mapping
        table_data = self.select_columns(valid_data, self.mappings[catalog_name])
        return table_data

    def format_df(self, df):
        """Format the raw data to match the database schema."""
        if "cell_phone" in df.columns:
            # Convert cell_phone column from Array[Struct(_Num, _Type)] to LongType
            get_cell_phone = F.filter(
                F.col("cell_phone"), lambda x: x["_Type"].isin(["C", "O"])
            )["_Num"][0]
            df = df.withColumn("cell_phone", get_cell_phone)
        if "home_phone" in df.columns:
            # Convert home_phone column from Array[Struct(_Num, _Type)] to LongType
            get_home_phone = F.filter(
                F.col("home_phone"), lambda x: x["_Type"].isin(["H"])
            )["_Num"][0]
            df = df.withColumn("home_phone", get_home_phone)
        if ("op_codes" in df.columns):
            # Convert op_code column from Array[Struct(_RecSvcOpCdDesc, _RecSvcOpCode)] to Array[Struct(op_code_desc, op_code)]
            df = df.withColumn(
                "op_codes",
                F.expr(
                    "transform(op_codes, x -> struct(x._RecSvcOpCode as op_code, x._RecSvcOpCdDesc as op_code_desc))"
                ),
            )
        return df

    def add_list_to_df(
        self, df, add_list, add_list_column_name, temp_col_name="order_temp"
    ):
        """Given a dataframe and a list, add the list as a column to the dataframe preserving order."""
        temp_df = self.spark.createDataFrame(
            [[x] for x in add_list], [add_list_column_name]
        ).withColumn(temp_col_name, F.monotonically_increasing_id())
        df = df.withColumn(temp_col_name, F.monotonically_increasing_id())
        return df.join(temp_df, [temp_col_name]).drop(F.col(temp_col_name))

    def insert_consumer(self, dealer_df, catalog_name):
        """Given a dataframe of dealer data insert into consumer table and return created ids."""
        desired_consumer_columns = self.mappings[catalog_name]["consumer"].keys()
        actual_consumer_columns = [
            x for x in desired_consumer_columns if x in dealer_df.columns
        ]
        actual_consumer_columns.append("dealer_id")

        consumer_df = dealer_df.select(actual_consumer_columns)
        insert_consumer_query = self.rds.get_insert_query_from_df(
            consumer_df, "consumer", "RETURNING id"
        )

        try:
            results = self.rds.commit_rds(insert_consumer_query)
            inserted_consumer_ids = [x[0] for x in results.fetchall()]
            return inserted_consumer_ids
        except Exception:
            logger.exception(f"Error running query: {insert_consumer_query}")
            raise

    def insert_vehicle_sale(self, dealer_df, catalog_name):
        """Given a dataframe of dealer data insert into vehicle sale table and return row count."""
        desired_vehicle_sale_columns = self.mappings[catalog_name][
            "vehicle_sale"
        ].keys()
        actual_vehicle_sale_columns = [
            x for x in desired_vehicle_sale_columns if x in dealer_df.columns
        ]
        actual_vehicle_sale_columns.append("dealer_id")
        actual_vehicle_sale_columns.append("consumer_id")

        vehicle_sale_df = dealer_df.select(actual_vehicle_sale_columns)
        insert_vehicle_sale_query = self.rds.get_insert_query_from_df(
            vehicle_sale_df,
            "vehicle_sale",
            "ON CONFLICT ON CONSTRAINT unique_vehicle_sale DO NOTHING RETURNING id",
        )
        try:
            results = self.rds.commit_rds(insert_vehicle_sale_query)
            inserted_vehicle_sale_ids = [x[0] for x in results.fetchall()]
            return inserted_vehicle_sale_ids
        except Exception:
            logger.exception(f"Error running query: {insert_vehicle_sale_query}")
            raise

    def insert_service_repair_order(self, dealer_df, catalog_name):
        """Given a dataframe of dealer data insert into service repair order table and return row count."""
        desired_service_repair_order_columns = self.mappings[catalog_name][
            "service_repair_order"
        ].keys()
        actual_service_repair_order_columns = [
            x for x in desired_service_repair_order_columns if x in dealer_df.columns
        ]
        actual_service_repair_order_columns.append("dealer_id")
        actual_service_repair_order_columns.append("consumer_id")

        service_repair_order_df = dealer_df.select(actual_service_repair_order_columns)
        insert_service_repair_order_query = self.rds.get_insert_query_from_df(
            service_repair_order_df,
            "service_repair_order",
            f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
            SET {', '.join([x + ' = excluded.' + x for x in desired_service_repair_order_columns])}
            RETURNING id""",
        )

        try:
            results = self.rds.commit_rds(insert_service_repair_order_query)
            inserted_service_repair_order_ids = [x[0] for x in results.fetchall()]
            return inserted_service_repair_order_ids
        except Exception:
            logger.exception(
                f"Error running query: {insert_service_repair_order_query}"
            )
            raise

    def insert_op_codes(self, dealer_df, catalog_name):
        """Given a dataframe of dealer data insert into op code table and return row count."""
        desired_op_code_columns = self.mappings[catalog_name]["op_codes"].keys()
        actual_op_code_columns = [
            x for x in desired_op_code_columns if x in dealer_df.columns
        ]
        actual_op_code_columns.append("dealer_id")

        combined_op_code_df = dealer_df.select(actual_op_code_columns).withColumn(
            "op_codes", F.explode("op_codes")
        )
        op_code_df = combined_op_code_df.select(
            F.col("op_codes.op_code").alias("op_code"),
            F.col("op_codes.op_code_desc").alias("op_code_desc"),
            "dealer_id",
        )
        insert_op_code_query = self.rds.get_insert_query_from_df(
            op_code_df,
            "op_code",
            f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
            SET {', '.join([x + ' = excluded.' + x for x in desired_op_code_columns])}
            RETURNING id""",
        )

        try:
            results = self.rds.commit_rds(insert_op_code_query)
            inserted_op_code_ids = [x[0] for x in results.fetchall()]
            return inserted_op_code_ids
        except Exception:
            logger.exception(f"Error running query: {insert_op_code_query}")
            raise

    def insert_op_code_repair_order(self, dealer_df):
        """Given a dataframe of dealer data insert into op code repair order table and return row count."""
        desired_op_code_repair_order_columns = ["op_codes", "repair_order_no"]
        actual_op_code_repair_order_columns = [
            x for x in desired_op_code_repair_order_columns if x in dealer_df.columns
        ]
        actual_op_code_repair_order_columns.append("dealer_id")

        op_code_repair_order_df = dealer_df.select(
            actual_op_code_repair_order_columns
        ).withColumn("op_codes", F.explode("op_codes"))
        op_code_repair_order_df = op_code_repair_order_df.select(
            F.col("op_codes.op_code").alias("op_code"), "dealer_id"
        )

        conditions_list = []
        for row in op_code_repair_order_df.collect():
            condition = f"""
                sro.dealer_id = {row['dealer_id']} 
                and sro.repair_order_no = '{row['repair_order_no']}' 
                and oc.op_code = '{row['op_code']}'
            """
            conditions_list.append(condition)
        conditions_str = ") or (".join(conditions_list)
        insert_op_code_repair_order_query = f"""
            insert into {self.schema}.op_code_repair_order (op_code_id, repair_order_id)
            select oc.id as op_code_id, sro.id as repair_order_id
            from {self.schema}.service_repair_order sro
            join {self.schema}.op_code oc on oc.dealer_id=sro.dealer_id
            where ({conditions_str})
        """
        try:
            results = self.rds.commit_rds(insert_op_code_repair_order_query)
            inserted_op_code_ids = [x[0] for x in results.fetchall()]
            return inserted_op_code_ids
        except Exception:
            logger.exception(
                f"Error running query: {insert_op_code_repair_order_query}"
            )
            raise

    def select_db_dealer_id(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_id_query = f"""
            select d.id from {self.schema}."dealer" d 
            join {self.schema}."integration_partner" i on d.integration_id = i.id 
            where d.dms_id = '{dms_id}' and i.name = '{self.integration}';"""
        results = self.rds.execute_rds(db_dealer_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No dealer {dms_id} found with query {db_dealer_id_query}."
            )
        else:
            return results[0]

    def upsert_df(self, df, catalog_name):
        """Upsert dataframe to RDS table."""
        insert_count = 0
        dealers = df.select("dms_id").distinct().collect()
        for dealer in dealers:
            dealer_df = df.filter(df.dms_id == dealer.dms_id)
            db_dealer_id = None
            try:
                db_dealer_id = self.select_db_dealer_id(dealer.dms_id)
                dealer_df = dealer_df.withColumn("dealer_id", F.lit(db_dealer_id))

                if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                    # Vehicle sale must insert into consumer table first
                    inserted_consumer_ids = self.insert_consumer(
                        dealer_df, catalog_name
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_id}"
                    )

                    # Then insert to vehicle sale with consumer ids
                    vehicle_sale_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )
                    vehicle_sale_ids = self.insert_vehicle_sale(
                        vehicle_sale_df, catalog_name
                    )
                    count = len(vehicle_sale_ids)
                    logger.info(
                        f"Added {count} rows to vehicle_sale for dealer {db_dealer_id}"
                    )
                    insert_count += count

                elif catalog_name == "reyreycrawlerdb_repair_order":
                    # Service repair order must insert into consumer table first
                    inserted_consumer_ids = self.insert_consumer(
                        dealer_df, catalog_name
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_id}"
                    )

                    # Then insert to service repair order with consumer ids
                    service_repair_order_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )
                    service_repair_order_ids = self.insert_service_repair_order(
                        service_repair_order_df, catalog_name
                    )
                    count = len(service_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to service_repair_order for dealer {db_dealer_id}"
                    )
                    insert_count += count

                    # Create op codes
                    inserted_op_code_ids = self.insert_op_codes(
                        service_repair_order_df, catalog_name
                    )
                    count = len(inserted_op_code_ids)
                    logger.info(
                        f"Added {count} rows to op_code for dealer {db_dealer_id}"
                    )

                    # Link op codes
                    inserted_op_code_repair_order_ids = (
                        self.insert_op_code_repair_order(service_repair_order_df)
                    )
                    count = len(inserted_op_code_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to op_code_repair_order for dealer {db_dealer_id}"
                    )
                raise RuntimeError("Force test exception")
            except Exception:
                logger.exception(
                    f"""Error: db_dealer_id {db_dealer_id}, dms_id {dealer.dms_id}, catalog {catalog_name} {df.schema}"""
                )
                # Send dealer_df to s3 and then sqs
                s3_uri = f"s3://{self.bucket_name}/{self.integration}/errors/{self.job_id}/{uuid.uuid4().hex}.csv"
                if "op_codes" in df.columns:
                    df = df.withColumn("op_codes", F.col("op_codes").cast("string"))
                df.write.options(header="True").csv(s3_uri, mode="overwrite")
                sqs_client = boto3.client("sqs")
                sqs_client.send_message(
                    QueueUrl=self.dlq_url, MessageBody=dumps({"path": s3_uri})
                )

        return insert_count

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            logger.info(f"The catalog is {catalog_name} and database is {self.database}")
            transformation_ctx = "datasource5_" + catalog_name
            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=transformation_ctx,
                groupFiles="none",
            )
            if datasource.count() != 0:
                df = (
                    datasource.toDF()
                    .withColumnRenamed("partition_0", "Year")
                    .withColumnRenamed("partition_1", "Month")
                    .withColumnRenamed("partition_2", "Date")
                )
                logger.info(df.schema)
                mapped_df = self.apply_mappings(df, catalog_name)
                formatted_df = self.format_df(mapped_df)
                upsert_count = self.upsert_df(formatted_df, catalog_name)
                logger.info(f"Added {upsert_count} rows.")
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
        job = ReyReyUpsertJob(job_id, args)
        job.run()
    except Exception:
        logger.exception("Error running ReyRey ETL.")
        raise
