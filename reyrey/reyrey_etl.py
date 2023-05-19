"""Rey Rey ETL Job."""
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
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window


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
            join {self.schema}."integration_partner" i on dip.integration_id = i.id 
            where dip.dms_id = '{dms_id}' and i.name = '{self.integration}' and dip.is_active = true;"""
        results = self.execute_rds(db_dealer_integration_partner_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No active dealer {dms_id} found with query {db_dealer_integration_partner_id_query}."
            )
        else:
            return results[0]

    def insert_op_code_repair_order(self, dealer_df):
        """Given a dataframe of dealer data insert into op code repair order table and return row count."""
        desired_op_code_repair_order_columns = ["op_codes", "repair_order_no"]
        actual_op_code_repair_order_columns = [
            x for x in desired_op_code_repair_order_columns if x in dealer_df.columns
        ]
        actual_op_code_repair_order_columns.append("dealer_integration_partner_id")

        op_code_repair_order_df = (
            dealer_df.select(actual_op_code_repair_order_columns)
            .withColumn("op_codes", F.explode("op_codes"))
            .select(
                F.col("op_codes.op_code").alias("op_code"),
                "dealer_integration_partner_id",
                "repair_order_no",
            )
        )

        conditions_list = []
        for row in op_code_repair_order_df.collect():
            condition = f"""
                sro.dealer_integration_partner_id = {row['dealer_integration_partner_id']} 
                and sro.repair_order_no = '{row['repair_order_no']}' 
                and oc.op_code = '{row['op_code']}'
            """
            conditions_list.append(condition)
        conditions_str = ") or (".join(conditions_list)
        insert_op_code_repair_order_query = f"""
            insert into {self.schema}.op_code_repair_order (op_code_id, repair_order_id)
            select oc.id as op_code_id, sro.id as repair_order_id
            from {self.schema}.service_repair_order sro
            join {self.schema}.op_code oc on oc.dealer_integration_partner_id=sro.dealer_integration_partner_id
            where ({conditions_str})
            RETURNING id
        """
        results = self.commit_rds(insert_op_code_repair_order_query)
        if results is None:
            return []
        inserted_op_code_ids = [x[0] for x in results.fetchall()]
        return inserted_op_code_ids

    def insert_op_codes(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into op code table and return row count."""
        desired_op_code_columns = mappings[catalog_name]["op_codes"].keys()
        actual_op_code_columns = [
            x for x in desired_op_code_columns if x in dealer_df.columns
        ]
        actual_op_code_columns.append("dealer_integration_partner_id")

        combined_op_code_df = dealer_df.select(actual_op_code_columns).withColumn(
            "op_codes", F.explode("op_codes")
        )
        unique_op_code_cols = [
            "dealer_integration_partner_id",
            "op_code",
            "op_code_desc",
        ]
        op_code_df = combined_op_code_df.select(
            F.col("op_codes.op_code").alias("op_code"),
            F.col("op_codes.op_code_desc").alias("op_code_desc"),
            "dealer_integration_partner_id",
        ).dropDuplicates(subset=unique_op_code_cols)
        insert_op_code_query = self.get_insert_query_from_df(
            op_code_df,
            "op_code",
            f"""ON CONFLICT ON CONSTRAINT unique_op_code DO UPDATE
            SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, op_code.{x})' for x in op_code_df.columns])}
            RETURNING id""",
        )

        results = self.commit_rds(insert_op_code_query)
        if results is None:
            return []
        inserted_op_code_ids = [x[0] for x in results.fetchall()]
        return inserted_op_code_ids

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
        actual_service_repair_order_columns.append("metadata")

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
        actual_vehicle_sale_columns.append("metadata")

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
        actual_consumer_columns.append("metadata")

        consumer_df = dealer_df.select(actual_consumer_columns)
        insert_consumer_query = self.get_insert_query_from_df(
            consumer_df, "consumer", "RETURNING id"
        )

        results = self.commit_rds(insert_consumer_query)
        if results is None:
            return []
        inserted_consumer_ids = [x[0] for x in results.fetchall()]
        return inserted_consumer_ids


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
        self.region = args["region"]
        self.is_prod = args["environment"] == "prod"
        self.integration = "reyrey"
        self.bucket_name = (
            f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        )
        self.rds = RDSInstance(self.is_prod, self.integration)
        self.mappings = {
            "reyreycrawlerdb_fi_closed_deal": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "consumer": {
                    "dealer_customer_no": "FIDeal.Buyer.CustRecord.ContactInfo._NameRecId",
                    "first_name": "FIDeal.Buyer.CustRecord.ContactInfo._FirstName",
                    "last_name": "FIDeal.Buyer.CustRecord.ContactInfo._LastName",
                    "email": "FIDeal.Buyer.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone.Array",
                    "postal_code": "FIDeal.Buyer.CustRecord.ContactInfo.Address.Array._Zip",
                    "home_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone.Array",
                },
                "vehicle_sale": {
                    "sale_date": "FIDeal.FIDealFin._CloseDealDate",
                    "listed_price": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
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
                    "service_package": "FIDeal.FIDealFin.WarrantyInfo.Array.ServiceCont._ServContYN",
                    "vehicle_gross": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    "extended_warranty": "FIDeal.FIDealFin.WarrantyInfo.Array.ExtWarranty.VehExtWarranty._ExpirationDate",
                    "vin": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin",
                    "model": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._ModelDesc",
                    "year": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr",
                    "delivery_date": "FIDeal.FIDealFin._DeliveryDate",
                },
            },
            "reyreycrawlerdb_repair_order": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "op_codes": {
                    "repair_order_no": "RepairOrder.RoRecord.Rogen._RoNo",
                    "op_codes": "RepairOrder.RoRecord.Rogen.RecommendedServc",
                },
                "consumer": {
                    "dealer_customer_no": "RepairOrder.CustRecord.ContactInfo._NameRecId",
                    "first_name": "RepairOrder.CustRecord.ContactInfo._FirstName",
                    "last_name": "RepairOrder.CustRecord.ContactInfo._LastName",
                    "email": "RepairOrder.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "RepairOrder.CustRecord.ContactInfo.phone.Array",
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
                    "comment": "RepairOrder.RoRecord.Rogen.RoCommentInfo._RoComment",
                    "recommendation": "RepairOrder.RoRecord.Rogen.TechRecommends._TechRecommend",
                },
            },
        }

    def apply_mappings(self, df, table_to_mappings):
        """Select valid db columns from a dataframe using dms column mappings, log and skip missing data."""
        ignore_columns = []
        selected_columns = []
        selected_column_names = []
        for db_columns_to_dms_columns in table_to_mappings.values():
            for db_column, dms_column in db_columns_to_dms_columns.items():
                try:
                    df.select(dms_column)
                except pyspark.sql.utils.AnalysisException:
                    ignore_columns.append(db_column)
                    logger.exception(
                        f"Column: {db_column} with mapping: {dms_column} not found in schema {df.schema.json()}."
                    )
                    raise

            for db_column, dms_column in db_columns_to_dms_columns.items():
                if (
                    db_column not in ignore_columns
                    and db_column not in selected_column_names
                ):
                    selected_columns.append(F.col(dms_column).alias(db_column))
                    selected_column_names.append(db_column)
        selected_columns.append("metadata")
        return df.select(selected_columns)

    def filter_null(self, df, catalog_name):
        """Map the raw data to the unified column and return as a dataframe."""
        if "reyreycrawlerdb_fi_closed_deal" == catalog_name:
            data_column_name = "FIDeal"
        elif "reyreycrawlerdb_repair_order" == catalog_name:
            data_column_name = "RepairOrder"
        else:
            raise RuntimeError(f"Unexpected catalog {catalog_name}")

        # Log data with null values
        null_data = df.filter(F.col(f"{data_column_name}.Array").isNull()).select(
            "Year",
            "Month",
            "Date",
            "ApplicationArea.Sender.DealerNumber",
            "ApplicationArea.BODId",
        )
        null_data_json = null_data.toJSON().collect()
        logging.warning(f"Skip processing null data: {null_data_json}")

        # Log and select data without null values
        valid_data = (
            df.filter(F.col(f"{data_column_name}.Array").isNotNull())
            .select(
                "Year",
                "Month",
                "Date",
                F.col("ApplicationArea.BODId").alias("BODId"),
                "ApplicationArea",
                F.explode(f"{data_column_name}.Array").alias(data_column_name),
            )
            .withColumn("Region", F.lit(self.region))
            .withColumn(
                "metadata",
                F.to_json(F.struct("Region", "Year", "Month", "Date", "BODId")),
            )
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
        return valid_data

    def format_df(self, df, catalog_name):
        """Format the raw data to match the database schema."""
        starting_count = df.count()
        if "cell_phone" in df.columns:
            # Convert cell_phone column from Array[Struct(_Num, _Type)] to LongType
            get_cell_phone = F.filter(
                F.col("cell_phone"), lambda x: x["_Type"].isin(["C", "O"])
            )["_Num"][0]
            if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                get_cell_phone = get_cell_phone["long"]
            df = df.withColumn("cell_phone", get_cell_phone)
        if "home_phone" in df.columns:
            # Convert home_phone column from Array[Struct(_Num, _Type)] to LongType
            get_home_phone = F.filter(
                F.col("home_phone"), lambda x: x["_Type"].isin(["H"])
            )["_Num"][0]
            if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                get_home_phone = get_home_phone["long"]
            df = df.withColumn("home_phone", get_home_phone)
        if "postal_code" in df.columns:
            # Convert postal_code column from Array[String] to String
            def calculate_postal_code(pyspark_arr):
                if pyspark_arr:
                    for raw_reyrey_str in pyspark_arr:
                        if raw_reyrey_str != "null":
                            return raw_reyrey_str
                    return None
                else:
                    return None

            if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                get_postal_code = F.udf(calculate_postal_code, StringType())
                df = df.withColumn("postal_code", get_postal_code(F.col("postal_code")))
        if "extended_warranty" in df.columns:
            # Convert extended_warranty column from Array[String] to JSON String
            def calculate_extended_warranty(pyspark_arr):
                if pyspark_arr:
                    warranty_expiration_date_strs = []
                    for raw_reyrey_str in pyspark_arr:
                        if raw_reyrey_str != "null":
                            warranty_expiration_date_strs.append(
                                f'{{"warranty_expiration_date":"{raw_reyrey_str}"}}'
                            )
                    return f"[{','.join(warranty_expiration_date_strs)}]"
                else:
                    return None

            get_extended_warranty = F.udf(calculate_extended_warranty, StringType())
            df = df.withColumn(
                "extended_warranty", get_extended_warranty(F.col("extended_warranty"))
            )
        if "service_package" in df.columns:
            # Convert service_package column from Array[String] to JSON String
            def calculate_service_package(pyspark_arr):
                if pyspark_arr:
                    has_service_contract_strs = []
                    for raw_reyrey_str in pyspark_arr:
                        if raw_reyrey_str != "null":
                            has_service_contract_strs.append(
                                f'{{"has_service_contract":"{raw_reyrey_str}"}}'
                            )
                    return f"[{','.join(has_service_contract_strs)}]"
                else:
                    return None

            get_service_package = F.udf(calculate_service_package, StringType())
            df = df.withColumn(
                "service_package", get_service_package(F.col("service_package"))
            )
        if "dealer_customer_no" in df.columns:
            if catalog_name == "reyreycrawlerdb_repair_order":
                # Convert List(int, string) to String
                df = df.withColumn(
                    "dealer_customer_no",
                    F.when(
                        F.col("dealer_customer_no.int").isNotNull(),
                        F.col("dealer_customer_no.int"),
                    ).otherwise(F.col("dealer_customer_no.string")),
                )
        if "txn_pay_type" in df.columns:
            # Convert Array[String] to String
            df = df.withColumn("txn_pay_type", F.concat_ws(",", F.col("txn_pay_type")))
        if "total_amount" in df.columns:
            # Convert Array[Double] to Double
            def calculate_sum_array(arr):
                if arr is not None:
                    return sum(x for x in arr if x is not None)
                else:
                    return None

            get_sum_array = F.udf(calculate_sum_array, DoubleType())
            df = df.withColumn("total_amount", get_sum_array(F.col("total_amount")))
        if "op_codes" in df.columns:
            # Convert op_code column from Array[Struct(_RecSvcOpCdDesc, _RecSvcOpCode)] to Array[Struct(op_code_desc, op_code)]
            df = df.withColumn(
                "op_codes",
                F.expr(
                    "transform(op_codes, x -> struct(x._RecSvcOpCode as op_code, x._RecSvcOpCdDesc as op_code_desc))"
                ),
            )

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
        original_count = df.count()
        temp_df = self.spark.createDataFrame(
            [[x] for x in add_list], [add_list_column_name]
        ).withColumn(temp_col_name, F.row_number().over(w))
        df = (
            df.withColumn(temp_col_name, F.row_number().over(w))
            .join(temp_df, [temp_col_name])
            .drop(F.col(temp_col_name))
        )

        if original_count != df.count():
            raise RuntimeError(
                f"Error adding list to df lost rows {original_count} to {df.count()} given list {add_list}"
            )
        return df

    def upsert_df(self, df, catalog_name):
        """Upsert dataframe to RDS table."""
        insert_count = 0
        dealers = df.select("dms_id").distinct().collect()
        for dealer in dealers:
            dealer_df = df.filter(df.dms_id == dealer.dms_id)
            db_dealer_integration_partner_id = None
            try:
                db_dealer_integration_partner_id = (
                    self.rds.select_db_dealer_integration_partner_id(dealer.dms_id)
                )
                dealer_df = dealer_df.withColumn(
                    "dealer_integration_partner_id",
                    F.lit(db_dealer_integration_partner_id),
                )
                if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                    # Vehicle sale must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )

                    # Then insert to vehicle sale with consumer ids
                    vehicle_sale_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    vehicle_sale_ids = self.rds.insert_vehicle_sale(
                        vehicle_sale_df, catalog_name, self.mappings
                    )
                    count = len(vehicle_sale_ids)
                    logger.info(
                        f"Added {count} rows to vehicle_sale for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

                elif catalog_name == "reyreycrawlerdb_repair_order":
                    # Service repair order must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )

                    # Then insert to service repair order with consumer ids
                    service_repair_order_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    service_repair_order_ids = self.rds.insert_service_repair_order(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(service_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to service_repair_order for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

                    # Create op codes
                    inserted_op_code_ids = self.rds.insert_op_codes(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(inserted_op_code_ids)
                    logger.info(
                        f"Added {count} rows to op_code for dealer {db_dealer_integration_partner_id}"
                    )

                    # Link op codes
                    inserted_op_code_repair_order_ids = (
                        self.rds.insert_op_code_repair_order(service_repair_order_df)
                    )
                    count = len(inserted_op_code_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to op_code_repair_order for dealer {db_dealer_integration_partner_id}"
                    )
            except Exception:
                logger.exception(
                    f"""Error: db_dealer_integration_partner_id {db_dealer_integration_partner_id}, dms_id {dealer.dms_id}, catalog {catalog_name} {df.schema}"""
                )
                s3_key = f"{self.integration}/errors/{dealer.dms_id}/{datetime.now().strftime('%Y-%m-%d')}/{self.job_id}/{uuid.uuid4().hex}.json"
                self.save_df_notify(df, s3_key)
                raise

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
            transformation_ctx = "bookmark_" + catalog_name
            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=transformation_ctx,
            )
            if datasource.count() != 0:
                try:
                    df = (
                        datasource.toDF()
                        .withColumnRenamed("partition_0", "Year")
                        .withColumnRenamed("partition_1", "Month")
                        .withColumnRenamed("partition_2", "Date")
                    )
                    # Filter rows with no data
                    df = self.filter_null(df, catalog_name)
                    # Get the base fields in a df via self.mappings
                    df = self.apply_mappings(df, self.mappings[catalog_name])
                    # Format necessary base fields to get a standardized df format
                    df = self.format_df(df, catalog_name)
                    # If you've properly formatted your df the next function should run without needing modifications per integration
                    upsert_count = self.upsert_df(df, catalog_name)
                    logger.info(
                        f"Added {upsert_count} total rows to primary table for {catalog_name}."
                    )
                except Exception:
                    s3_key = f"{self.integration}/errors/job/{datetime.now().strftime('%Y-%m-%d')}/{self.job_id}/{uuid.uuid4().hex}.json"
                    self.save_df_notify(df, s3_key)
                    raise
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
            "region",
        ],
    )

    job_id = args["JOB_RUN_ID"]
    logging.basicConfig(
        format="reyrey_" + str(job_id) + " %(asctime)s %(levelname)s: %(message)s",
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
