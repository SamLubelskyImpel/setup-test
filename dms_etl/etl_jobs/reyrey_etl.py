"""Rey Rey ETL Job."""

import sys
from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.functions import col, lit, flatten, explode
from awsglue.job import Job
from json import loads
import psycopg2.pool



def _load_secret():
    """Get DMS DB configuration from Secrets Manager."""

    secret_string = loads(SM_CLIENT.get_secret_value(
        SecretId='prod/DMSDB' if isprod else 'stage/DMSDB' 
    )['SecretString'])

    DB_CONFIG = {
        'db_name': secret_string['db_name'],
        'host': secret_string['host'],
        'user': secret_string['user'],
        'password': secret_string['password'],
        'port': '5432'
    }

    return DB_CONFIG


class ReyReyUpsertJob:
    """Create object to perform ETL."""


    def __init__(self, args, pool):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(args['JOB_NAME'], args)
        self.catalog_table_names = args['catalog_table_names'].split(',')
        self.upsert_table_order = self.get_upsert_table_order()
        self.pool = pool

    def get_upsert_table_order(self):
        """Return a list of tables to upsert by order of dependency."""
        upsert_table_order = {}
        for name in self.catalog_table_names:
            if 'fi_closed_deal' in name:
                upsert_table_order[name] = ['dealer', 'consumer', 'vehicle', 'vehicle_sale']
            elif 'repair_order' in name:
                upsert_table_order[name] = ['dealer', 'consumer', 'vehicle', 'service_repair_order']

        return upsert_table_order


    def apply_mappings(self, df,  tablename, catalog_table):
        """Apply appropriate mapping to dynamic frame."""
        #for all of the comments in each mapping this customization can be added in the partition methods.
        mapping = []
        current_df = df.toDF()
        dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

        if 'fi_closed_deal' in catalog_table:
            if tablename == 'dealer':

                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")
                          
                #before upserting needs to be checked for existence
            elif tablename == 'consumer':

                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.phone").alias("phone"),
                ).withColumn("DealerNumber", lit(dealer_number))


                #additional work for grabbing the phone number might be required
                #check if consumer exists before upsert. grab dealer id before upserting
            elif tablename == 'vehicle':

                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin").alias("vin"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr").alias("year"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._VehClass").alias("vehicle_class")
                ).withColumn("DealerNumber", lit(dealer_number))


                #for vehicle before inserting we need to find the appropriate dealer id. if it doesn't exist we need to create it and grab it.
                #before upsert dealer id needs to be grabbed
            elif tablename == 'vehicle_sale':
                
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

                
                #bool values newused need to be extracted to appropriate types
                #dump warranty info as a json into the table in the catch all field
                #vehicle id(use vin) and dealer id(use dealernumber) needs to be grabbed before upsert


        elif 'repair_order' in catalog_table:
            if tablename == 'dealer':

                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")
                          
                #before upserting needs to be checked for existence
            elif tablename == 'vehicle':

                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin")
                ).withColumn("DealerNumber", lit(dealer_number))

         
                #before upsert dealer id needs to be grabbed.
            elif tablename == 'consumer':

                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )

                # Select the nested columns
                current_df = current_df.select(
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("RepairOrder.CustRecord.ContactInfo.phone").alias("phone"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email"),
                    col("RepairOrder.CustRecord.ContactInfo.Address").alias("address")
                )


                #check if consumer exists before upsert. check by email 
            elif tablename == 'service_repair_order':

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

 
                #before upsert vehicle id needs to be grabbed by the vin and included. dealer id(use dealer number) also needs to be included. consumer id(use email) also needs to be included.
        
        # Convert the flattened DataFrame back to a DynamicFrame
        current_dyf = DynamicFrame.fromDF(
            current_df, self.glueContext, "current_dyf"
        ) 
        return current_dyf


    def read_data_from_catalog(self, database, catalog_table):
        """Read data from the AWS catalog and return a dynamic frame."""
        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=catalog_table,
            transformation_ctx="datasource0"
        )


    def upsert_consumer(self, df):
        """Upsert consumer data."""
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            for _, row in df.iterrows():
                try:
                    #grab the dealer id we need
                    cursor.execute("SELECT COUNT(*), dealer_id FROM dealer WHERE dms_id=%s", (row['DealerNumber']))
                    dealers = cursor.fetchone()

                    if dealers[0] > 0:
                        #check if the consumer already exists
                        cursor.execute("SELECT COUNT(*) FROM consumer WHERE dealer_id=%s AND phone=%s AND email=%s AND firstname=%s AND lastname=%s AND postal_code=%s", (
                            dealers[1], row['phone'], row['email'], row['firstname'], row['lastname'], row['postal_code']
                            ))
                        result = cursor.fetchone()
                        if result[0] == 0:
                            cursor.execute("""
                                INSERT INTO consumer (firstname, lastname, phone, postal_code, email, dealer_id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (row['firstname'], row['lastname'], row['phone'], row['postal_code'], row['email'], result[1]))

                    conn.commit()
                except Exception as e:
                    logging.error(f"Error processing row: {row}. Error: {e}")
                    conn.rollback()
            cursor.close()
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            raise e
        finally:
            self.pool.putconn(conn)


    def upsert_dealer(self, df):
        """Upsert dealer data."""
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            for _, row in df.iterrows():
                try:
                    cursor.execute("SELECT COUNT(*) FROM dealer WHERE dms_id=%s", (row['DealerNumber']))
                    result = cursor.fetchone()

                    if result[0] == 0:
                        cursor.execute("""
                            INSERT INTO dealer (dms_id)
                            VALUES (%s)
                        """, (row['DealerNumber']))

                    conn.commit()
                except Exception as e:
                    logging.error(f"Error processing row: {row}. Error: {e}")
                    conn.rollback()
            cursor.close()
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            raise e
        finally:
            self.pool.putconn(conn)


    def upsert_vehicle(self, df, catalog):
        """Upsert vehicle data."""
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            for _, row in df.iterrows():
                try:
                    cursor.execute("SELECT COUNT(*), dealer_id FROM dealer WHERE dms_id=%s", (row['DealerNumber']))
                    dealer = cursor.fetchone()

                    if dealer[0] > 0:
                        #check if the vehicle already exists
                        cursor.execute("SELECT COUNT(*), vehicle_id FROM vehicle WHERE dealer_id=%s AND vin=%s", (
                            dealers[1], row['vin']
                            ))
                        result = cursor.fetchone()

                        #if it doesn't add it. If it does and the catalog we're upsertting to is fi. We have additional data for vehicle. 
                        if result[0] == 0:
                            cursor.execute("""
                                INSERT INTO vehicle (dealer_id, vin)
                                VALUES (%s)
                            """, (dealers[1], row['vin']))

                        elif result[0] > 0 and catalog == 'fi_closed_deal':
                            cursor.execute("""
                                UPDATE vehicle
                                SET vehicle_class=%s, year=%s
                                WHERE vehicle_id=%s
                            """, (row['vehicle_class'], row['year'], result[1]))                        


                    conn.commit()
                except Exception as e:
                    logging.error(f"Error processing row: {row}. Error: {e}")
                    conn.rollback()
            cursor.close()
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            raise e
        finally:
            self.pool.putconn(conn)



    def run(self, database):
        """Run ETL for each table in our catalog."""

        for catalog_table in self.catalog_table_names:

            for table_name in self.upsert_table_order[catalog_table]:
                datasource0 = self.read_data_from_catalog(database=database, catalog_table=catalog_table)

                # Apply mappings to the DynamicFrame
                df_transformed = self.apply_mappings(datasource0, table_name, catalog_table)

                # Convert the DynamicFrame to a DataFrame
                df = df_transformed.toDF()


                # Perform upsert based on the table_name
                if table_name == 'consumer':
                    self.upsert_consumer(df)
                elif table_name == 'vehicle':
                    self.upsert_vehicle(df, catalog_table)
                # elif table_name == 'vehicle_sale':
                #     df.rdd.foreachPartition(self.upsert_vehicle_sale_partition)
                elif table_name == 'dealer':
                    self.upsert_dealer(df)
                # elif table_name == 'service_repair_order':
                #     df.rdd.foreachPartition(self.upsert_deal_partition)
                else:
                    raise ValueError(f"Invalid table name: {table_name}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "db_name", "catalog_table_names", "catalog_connection", "environment"])
    SM_CLIENT = boto3.client('secretsmanager')
    isprod = args["environment"] == 'prod'
    # Create database connection pool
    DB_CREDENTIALS = _load_secret()

    pool = psycopg2.pool.SimpleConnectionPool(
        1,
        5,
        dbname=DB_CREDENTIALS['db_name'],
        host=DB_CREDENTIALS['host'],
        port=DB_CREDENTIALS['port'],
        user=DB_CREDENTIALS['user'],
        password=DB_CREDENTIALS['password']
    )
    
    job = ReyReyUpsertJob(args, pool=pool)    
    job.run(database=args["db_name"])