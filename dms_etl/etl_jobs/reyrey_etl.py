"""Rey Rey ETL Job."""

from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame 
import datetime
from json import dumps, loads
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, flatten, explode
import psycopg2.pool
import sys




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
        mapping = []
        current_df = df.toDF()
        dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

        if 'fi_closed_deal' in catalog_table:

            if tablename == 'dealer':
                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")
                          
            elif tablename == 'consumer':
                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )
                current_df = current_df.select(
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.phone").alias("phone"),
                ).withColumn("DealerNumber", lit(dealer_number))

            elif tablename == 'vehicle':
                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin").alias("vin"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr").alias("year"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._VehClass").alias("vehicle_class")
                ).withColumn("DealerNumber", lit(dealer_number))

            elif tablename == 'vehicle_sale':
                current_df = current_df.select(
                    explode("FIDeal").alias("FIDeal")
                )
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle._VehCost").alias("cost_of_vehicle"),
                    col("FIDeal.FIDealFin.TransactionVehicle._Discount").alias("discount_on_price"),
                    col("FIDeal.FIDealFin.TransactionVehicle._InspectionDate").alias("date_of_state_inspection"),
                    col("FIDeal.FIDealFin.TransactionVehicle._DaysInStock").alias("days_in_stock"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin").alias("vin"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._OdomReading").alias("mileage_on_vehicle"),
                    col("FIDeal.FIDealFin.TradeIn._ActualCashValue").alias("trade_in_value"),
                    col("FIDeal.FIDealFin.TradeIn._Payoff").alias("payoff_on_trade"),
                    col("FIDeal.FIDealFin.Recap.Reserves._NetProfit").alias("profit_on_sale"),
                    col("FIDeal.FIDealFin.Recap.Reserves._VehicleGross").alias("vehicle_gross"),
                    col("FIDeal.FIDealFin.WarrantyInfo").alias("warranty_info"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._NewUsed").alias("NewUsed"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.phone").alias("phone"),
                ).withColumn("DealerNumber", lit(dealer_number))

        elif 'repair_order' in catalog_table:
            if tablename == 'dealer':
                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")
                        
            elif tablename == 'vehicle':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin")
                ).withColumn("DealerNumber", lit(dealer_number))
                
            elif tablename == 'consumer':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("RepairOrder.CustRecord.ContactInfo.phone").alias("phone"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email"),
                    col("RepairOrder.CustRecord.ContactInfo.Address").alias("address")
                ).withColumn("DealerNumber", lit(dealer_number))

            elif tablename == 'service_repair_order':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin"),
                    col("RepairOrder.RoRecord.Rogen._RoNo").alias("repair_order_no"),
                    col("RepairOrder.RoRecord.Rogen._CustRoTotalAmt").alias("consumer_total_amount"),
                    col("RepairOrder.RoRecord.Rogen._RoCreateDate").alias("repair_order_open_date"),
                    col("RepairOrder.RoRecord.Rogen.TechRecommends._TechRecommend").alias("recommendation"),
                    col("RepairOrder.ServVehicle.VehicleServInfo._LastRODate").alias("repair_order_close_date"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email"),
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname")
                ).withColumn("DealerNumber", lit(dealer_number))

         
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


    def upsert_consumer(self, cursor, row, phone, email, postal_code):
        """Upsert consumer data."""

        #grab the dealer id we need
        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealers = cursor.fetchone()

        if dealers is not None:
            dealer_id = dealers[0]
            #check if the consumer already exists
            cursor.execute("SELECT COUNT(*) FROM consumer WHERE dealer_id=%s AND email=%s", (
                dealer_id, email
                ))
            result = cursor.fetchone()
            if result[0] == 0:
                cursor.execute("""
                    INSERT INTO consumer (first_name, last_name, home_phone, postal_code, email, dealer_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (row['firstname'], row['lastname'], phone, postal_code, email, dealer_id))


    def upsert_dealer(self, cursor, row):
        """Upsert dealer data."""

        cursor.execute("SELECT COUNT(*) FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        result = cursor.fetchone()

        if result[0] == 0:
            cursor.execute("""
                INSERT INTO dealer (dms_id)
                VALUES (%s)
            """, (row['DealerNumber'],))


    def upsert_vehicle(self, cursor, row, catalog):
        """Upsert vehicle data."""

        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()

        if dealer is not None:
            dealer_id = dealer[0]           
            #check if the vehicle already exists
            cursor.execute("SELECT id FROM vehicle WHERE dealer_id=%s AND vin=%s", (
                dealer_id, row['vin']
                ))
            vehicle = cursor.fetchone()

            #if it doesn't add it. If it does and the catalog we're upsertting to is fi. We have additional data for vehicle. 
            if vehicle is None:
                cursor.execute("""
                    INSERT INTO vehicle (dealer_id, vin)
                    VALUES (%s, %s)
                """, (dealer_id, row['vin']))

            elif vehicle is not None and catalog == 'fi_closed_deal':
                vehicle_id = vehicle[0]
                cursor.execute("""
                    UPDATE vehicle
                    SET vehicle_class=%s, year=%s
                    WHERE vehicle_id=%s
                """, (row['vehicle_class'], row['year'], vehicle_id))    
            elif vehicle is None and catalog == 'fi_closed_deal':
                cursor.execute("""
                    INSERT INTO vehicle (dealer_id, vin, vehicle_class, year)
                    VALUES (%s, %s, %s, %s)
                """, (dealer_id, row['vin'], row['vehicle_class'], row['year']))
 

    def upsert_vehicle_sale(self, cursor, row, warranty_info, date_of_state_inspection, is_new, phone, email, postal_code):
        """Upsert Vehicle Sale data."""

        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()

        cursor.execute("SELECT id FROM vehicle WHERE vin=%s", (row['vin'],))
        vehicle = cursor.fetchone()

        #prepare NewUsed and Warranty info

        if dealer is not None and vehicle is not None:
            dealer_id = dealer[0]           
            vehicle_id = vehicle[0]           
            cursor.execute("SELECT id FROM consumer WHERE dealer_id=%s AND email=%s", (
                dealer_id, email
                ))

            consumer = cursor.fetchone()
            if consumer is not None:
                consumer_id = consumer[0]           

                cursor.execute("""
                    INSERT INTO vehicle_sale (
                        cost_of_vehicle, discount_on_price,
                        date_of_state_inspection, days_in_stock,
                        mileage_on_vehicle, trade_in_value,
                        payoff_on_trade, profit_on_sale,
                        vehicle_gross, extended_warranty, is_new,
                        vehicle_id, dealer_id, consumer_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['cost_of_vehicle'], row['discount_on_price'],
                date_of_state_inspection, row['days_in_stock'], row['mileage_on_vehicle'],
                row['trade_in_value'], row['payoff_on_trade'], row['profit_on_sale'], 
                row['vehicle_gross'], warranty_info, is_new,
                vehicle_id, dealer_id, consumer_id))


    def upsert_service_repair_order(self, cursor, row, email, phone, postal_code, repair_order_open_date, repair_order_close_date):
        """Upsert Service Repair Order to RDS."""
       
        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()

        cursor.execute("SELECT id FROM vehicle WHERE vin=%s", (row['vin'],))
        vehicle = cursor.fetchone()
        if dealer is not None:
            dealer_id = dealer[0]           
            vehicle_id = vehicle[0]           

            cursor.execute("SELECT id FROM consumer WHERE dealer_id=%s AND email=%s", (
                dealer_id, email
                ))
            consumer = cursor.fetchone()
            if consumer is not None:
                consumer_id = consumer[0]           

                cursor.execute("""
                    INSERT INTO service_repair_order (
                        repair_order_no, ro_open_date,
                        recommendation, ro_close_date,
                        consumer_total_amount,
                        vehicle_id, dealer_id, consumer_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['repair_order_no'], repair_order_open_date,
                row['recommendation'], repair_order_close_date,
                row['consumer_total_amount'],
                vehicle_id, dealer_id, consumer_id
                ))


    def format_phone_number(self, row):
        phone = None
        if hasattr(row, 'phone'):
            if hasattr(row.phone, 'array') and row.phone.array is not None:
                phone = next((phone_row._Num.long for phone_row in row.phone.array if phone_row._Type == 'H'), None)
            elif hasattr(row.phone, 'struct') and row.phone.struct is not None:
                if row.phone.struct._Type == 'H':
                    phone = row.phone.struct._Num

        return str(phone) if phone is not None else phone


    def format_email(self, row):
        email = None
        if hasattr(row, 'email') and row.email is not None:
            email = row.email._MailTo 
        return str(email) if email is not None else email

    def format_postal_code(self, row):
        postal_code = None
        if hasattr(row, 'address') and row.address is not None:
            if hasattr(row.address, 'struct') and row.address.struct is not None:
                postal_code = row.address.struct._Zip
            elif hasattr(row.address, 'array') and row.address.array is not None:
                postal_code = next((address_row._Zip for address_row in row.address.array), None)

        return str(postal_code) if postal_code is not None else postal_code

    def format_warranty_info(self, row):
        has_array = False
        has_struct = False
        warranty_info_json = None
        if hasattr(row, 'warranty_info') and row.warranty_info is not None:
            if hasattr(row.warranty_info, 'array') and row.warranty_info.array is not None:
                warranty_info_json = dumps([item.asDict(True) for item in row.warranty_info.array]) 
            elif hasattr(row.warranty_info, 'struct') and row.warranty_info.struct is not None:
                warranty_info_json = dumps(row.warranty_info.struct.asDict(True)) 
        
        return warranty_info_json

    def format_is_new(self, row):
        is_new = False
        if hasattr(row, 'NewUsed'):
            is_new = True if row.NewUsed == 'N' else False
        return is_new

    def format_date(self, row, attribute_name):
        ro_date = None
        if hasattr(row, attribute_name):
            if attribute_name == 'repair_order_close_date' and row.repair_order_close_date is not None:
                dt_obj = datetime.datetime.strptime(row.repair_order_close_date, '%m/%d/%Y')
                ro_date = dt_obj.strftime('%Y-%m-%d')

            elif attribute_name == 'repair_order_open_date' and row.repair_order_open_date is not None: 
                dt_obj = datetime.datetime.strptime(row.repair_order_open_date, '%m/%d/%Y')
                ro_date = dt_obj.strftime('%Y-%m-%d')

            elif attribute_name == 'date_of_state_inspection' and row.date_of_state_inspection is not None:
                dt_obj = datetime.datetime.strptime(row.date_of_state_inspection, '%m/%d/%Y')
                ro_date = dt_obj.strftime('%Y-%m-%d')

        return ro_date

    

    def upsert(self, df, table_name, catalog_table):
        """Upsert ReyRey data to RDS."""

        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()

            for row in df.rdd.toLocalIterator():
                try:
                    if table_name == 'consumer':
                        phone = self.format_phone_number(row)
                        email = self.format_email(row)
                        postal_code = self.format_postal_code(row)
                        self.upsert_consumer(cursor, row, phone, email, postal_code)
                    elif table_name == 'vehicle':
                        self.upsert_vehicle(cursor, row, catalog_table)
                    elif table_name == 'vehicle_sale':
                        email = self.format_email(row)
                        phone = self.format_phone_number(row)
                        postal_code = self.format_postal_code(row)
                        warranty_info = self.format_warranty_info(row)
                        date_of_state_inspection = self.format_date(row, 'date_of_state_inspection')
                        is_new = self.format_is_new(row)
                        self.upsert_vehicle_sale(cursor, row, warranty_info, date_of_state_inspection, is_new, phone, email, postal_code)
                    elif table_name == 'dealer':
                        self.upsert_dealer(cursor, row)
                    elif table_name == 'service_repair_order':
                        email = self.format_email(row)
                        phone = self.format_phone_number(row)
                        postal_code = self.format_postal_code(row)
                        repair_order_open_date = self.format_date(row, 'repair_order_open_date')
                        repair_order_close_date = self.format_date(row, 'repair_order_close_date')
                        self.upsert_service_repair_order(cursor, row, email, phone, postal_code, repair_order_open_date, repair_order_close_date)
                    else:
                        raise ValueError(f"Invalid table name: {table_name}")

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
                self.upsert(df, table_name, catalog_table)



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