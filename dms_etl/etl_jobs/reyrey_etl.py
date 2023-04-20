"""Rey Rey ETL Job."""

import sys
from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, lit, flatten, explode
from awsglue.job import Job
from json import loads


class ReyReyUpsertJob:
    """Create object to perform ETL."""


    def __init__(self, args):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(args['JOB_NAME'], args)
        self.DB_CONFIG = self._load_secret()
        self.catalog_table_names = args['catalog_table_names'].split(',')
        print(self.catalog_table_names)
        self.upsert_table_order = self.get_upsert_table_order()


    def get_upsert_table_order(self):
        """Return a list of tables to upsert by order of dependency."""

        upsert_table_order = {}
        for name in self.catalog_table_names:
            if 'fi_closed_deal' in name:
                upsert_table_order[name] = ['consumer', 'vehicle', 'vehicle_sale']
            elif 'repair_order' in name:
                upsert_table_order[name] = ['dealer', 'vehicle', 'consumer', 'service_repair_order']

        return upsert_table_order


    def _load_secret(self):
        """Get DMS DB configuration from Secrets Manager."""

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

    def apply_mappings(self, df,  tablename, catalog_table):
        """Apply appropriate mapping to dynamic frame."""
        #for all of the comments in each mapping this customization can be added in the partition methods.
        mapping = []

        if 'fi_closed_deal' in catalog_table:
            if tablename == 'consumer':
                # Flatten the data and extract the CustRecord fields
                conrecord_df = df.toDF()
                dealer_number = conrecord_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

                conrecord_df = conrecord_df.select(
                    explode("FIDeal").alias("FIDeal")
                )

                # Select the nested columns
                conrecord_df = conrecord_df.select(
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("LastName"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("FirstName"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address")
                )

                # Print the flattened DataFrame
                conrecord_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # conrecord_dyf = DynamicFrame.fromDF(
                #     conrecord_df, glueContext, "conrecord_dyf"
                # )

                #additional work for grabbing the phone number might be required
                #check if consumer exists before upsert
            elif tablename == 'vehicle':
                current_df = df.toDF()
                dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin").alias("vin"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr").alias("year"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._VehClass").alias("vehicle_class")
                ).withColumn("DealerNumber", lit(dealer_number))

                current_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )

                #for vehicle before inserting we need to find the appropriate dealer id. if it doesn't exist we need to create it and grab it.
                #before upsert dealer id needs to be grabbed
            elif tablename == 'vehicle_sale':

                current_df = df.toDF()
                dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle._VehCost").alias("cost_of_vehicle"),
                    col("FIDeal.FIDealFin.TransactionVehicle._Discount").alias("discount_on_price"),
                    col("FIDeal.FIDealFin.TransactionVehicle._InspectionDate").alias("date_of_state_inspection"),
                    col("FIDeal.FIDealFin.TransactionVehicle._DaysInStock").alias("days_in_stock"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._OdomReading").alias("mileage_on_vehicle"),
                    col("FIDeal.FIDealFin.TradeIn._ActualCashValue").alias("trade_in_value"),
                    col("FIDeal.FIDealFin.TradeIn._Payoff").alias("payoff_on_trade"),
                    col("FIDeal.FIDealFin.Recap.Reserves._NetProfit").alias("profit_on_sale"),
                    col("FIDeal.FIDealFin.Recap.Reserves._VehicleGross").alias("vehicle_gross"),
                    col("FIDeal.FIDealFin.WarrantyInfo").alias("warranty_info"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._NewUsed").alias("NewUsed"),

                ).withColumn("DealerNumber", lit(dealer_number))

                current_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )
                
                #bool values newused need to be extracted to appropriate types
                #dump warranty info as a json into the table in the catch all field
                #vehicle id(use vin) and dealer id(use dealernumber) needs to be grabbed before upsert


        elif 'repair_order' in catalog_table:
            if tablename == 'dealer':
                current_df = df.toDF()

                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")
                
                current_df.show()

                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )
                              
                #before upserting needs to be checked for existence
            elif tablename == 'vehicle':
                current_df = df.toDF()
                dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]
                current_df.printSchema()

                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin")
                ).withColumn("DealerNumber", lit(dealer_number))

                current_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )
                                  
                #before upsert dealer id needs to be grabbed.
            elif tablename == 'consumer':
                current_df = df.toDF()

                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("RepairOrder.CustRecord.ContactInfo.phone").alias("phone"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email")
                )

                current_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )
                      

                #check if consumer exists before upsert. check by email 
            elif tablename == 'service_repair_order':

                current_df = df.toDF()
                dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin"),
                    col("RepairOrder.RoRecord.Rogen._RoNo").alias("repair_order_no"),
                    col("RepairOrder.RoRecord.Rogen._RoCreateDate").alias("repair_order_open_date"),
                    col("RepairOrder.RoRecord.Rogen.RecommendedServc").alias("recommendation"),
                    col("RepairOrder.ServVehicle.VehicleServInfo._LastRODate").alias("repair_order_close_date"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email")
                ).withColumn("DealerNumber", lit(dealer_number))

                current_df.show()

                # Convert the flattened DataFrame back to a DynamicFrame
                # current_dyf = DynamicFrame.fromDF(
                #     current_df, glueContext, "current_dyf"
                # )
                      
                #before upsert vehicle id needs to be grabbed by the vin and included. dealer id(use dealer number) also needs to be included. consumer id(use email) also needs to be included.
      
        # RETURN DYNAMIC FRAME
        # return ApplyMapping.apply(frame=df, mappings=mappings, transformation_ctx=f"applymapping_{tablename}")


    def read_data_from_catalog(self, database, catalog_table):
        """Read data from the AWS catalog and return a dynamic frame."""

        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=catalog_table,
            transformation_ctx="datasource0"
        )


    def run(self, database):
        """Run ETL for each table in our catalog."""

        for catalog_table in self.catalog_table_names:
            print(self.upsert_table_order[catalog_table])
            for table_name in self.upsert_table_order[catalog_table]:
                datasource0 = self.read_data_from_catalog(database=database, catalog_table=catalog_table)

                # Apply mappings to the DynamicFrame
                df_transformed = self.apply_mappings(datasource0, table_name, catalog_table)

                # Convert DynamicFrame to DataFrame
                # df_transformed.toDF().show()


                # # Perform upsert based on the table_name
                # if table_name == 'consumer':
                #     df.rdd.foreachPartition(self.upsert_consumer_partition)
                # elif table_name == 'vehicle':
                #     df.rdd.foreachPartition(self.upsert_vehicle_partition)
                # elif table_name == 'vehicle_sale':
                #     df.rdd.foreachPartition(self.upsert_vehicle_sale_partition)
                # elif table_name == 'dealer':
                #     df.rdd.foreachPartition(self.upsert_dealer_partition)
                # elif table_name == 'service_repair_order':
                #     df.rdd.foreachPartition(self.upsert_deal_partition)
                # else:
                #     raise ValueError(f"Invalid table name: {table_name}")


if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "db_name", "catalog_table_names", "catalog_connection", "environment"])
    SM_CLIENT = boto3.client('secretsmanager')
    isprod = args["environment"] == 'prod'
    job = ReyReyUpsertJob(args)    
    job.run(database=args["db_name"])