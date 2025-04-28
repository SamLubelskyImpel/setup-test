from datetime import datetime
from utils import xml_to_string, execute_ordered_threads, get_xml_tag_text
import logging
import boto3
import os
from json import dumps
from api_wrapper import DealerTrackApi


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

S3 = boto3.client("s3")
IS_PROD = os.environ.get("ENVIRONMENT") == "prod"
INTEGRATIONS_BUCKET = os.environ.get("INTEGRATIONS_BUCKET")


class DataPullManager:

    def __init__(self, dms_id: str, date: str, resource: str):
        self.date = datetime.strptime(date, "%Y-%m-%d")
        self.resource = resource
        self.dms_id = dms_id
        self.enterprise_code, self.company_number = dms_id.split("_")
        self.api = DealerTrackApi(self.enterprise_code, self.company_number)


    def get_op_codes(self):
        op_codes = self.api.op_codes_table()
        return [xml_to_string(op_code) for op_code in op_codes]


    def start(self):
        if self.resource == "service_appointment":
            self.op_codes = self.get_op_codes()
            self.__appointments_pull()
        elif self.resource == "fi_closed_deal":
            self.__deals_pull()
        elif self.resource == "repair_order":
            self.op_codes = self.get_op_codes()
            self.__ro_pull()
        else:
            raise Exception(f"Resource {self.resource} not supported")


    def __upload_to_s3(self, file_content: dict):
        key = f"dealertrack-dms/raw/{self.resource}/{self.dms_id}/{self.date.year}/{self.date.month}/{self.date.day}/{self.date.isoformat()}_{self.resource}.json"
        S3.put_object(
            Body=dumps(file_content),
            Bucket=INTEGRATIONS_BUCKET,
            Key=key
        )
        logger.info(f"Uploaded {self.resource} to {key}")


    def __get_from_set(self, resources_set: set, resource: str):
        if resource == "vehicles":
            resources_function = self.api.vehicle_lookup
        elif resource == "customers":
            resources_function = self.api.customer_lookup
        elif resource == "deals":
            resources_function = self.api.deal_lookup
        elif resource == "repair_orders":
            resources_function = self.api.ro_details

        result = {}

        for value in resources_set:
            response = resources_function(value)
            result[value] = xml_to_string(response)

        return result


    def __appointments_pull(self):
        lookup_result = self.api.appointment_lookup(self.date)

        unique_vins = set()
        unique_customers = set()
        appointments = []

        for result in lookup_result:
            vin = get_xml_tag_text(result, 'VIN')
            customer_number = get_xml_tag_text(result, 'CustomerKey')

            if not vin or not customer_number:
                continue

            unique_vins.add(vin)
            unique_customers.add(customer_number)
            appointments.append(xml_to_string(result))

        vehicles, customers = execute_ordered_threads(tasks=[
            (self.__get_from_set, unique_vins, "vehicles"),
            (self.__get_from_set, unique_customers, "customers")
        ], max_workers=2)

        file_content = {
            "root_appointments": appointments,
            "vehicles": vehicles,
            "customers": customers,
            "op_codes": self.op_codes
        }

        self.__upload_to_s3(file_content)


    def __deals_pull(self):
        search_result = self.api.deal_search(self.date)

        unique_vins = set()
        unique_customers = set()
        unique_deals = set()
        deals = []

        for result in search_result:
            deal_number = get_xml_tag_text(result, 'DealNumber')
            vin = get_xml_tag_text(result, 'VIN')
            customer_number = get_xml_tag_text(result, 'BuyerCustomerNumber')
            cobuyer_customer_number = get_xml_tag_text(result, 'CoBuyerCustomerNumber')

            if not deal_number or not vin or not customer_number: continue

            unique_vins.add(vin)
            unique_customers.add(customer_number)
            if cobuyer_customer_number and cobuyer_customer_number != '0':
                unique_customers.add(cobuyer_customer_number)
            unique_deals.add(deal_number)
            deals.append(xml_to_string(result))

        vehicles, customers, deals_details = execute_ordered_threads(tasks=[
            (self.__get_from_set, unique_vins, "vehicles"),
            (self.__get_from_set, unique_customers, "customers"),
            (self.__get_from_set, unique_deals, "deals")
        ], max_workers=3)

        file_content = {
            "root_deals": deals,
            "vehicles": vehicles,
            "customers": customers,
            "deals_details": deals_details
        }

        self.__upload_to_s3(file_content)


    def __ro_pull(self):
        lookup_result = self.api.ro_lookup(self.date)

        unique_vins = set()
        unique_customers = set()
        unique_ros = set()
        ros = []

        for result in lookup_result:
            ro_number = get_xml_tag_text(result, 'RepairOrderNumber', is_ro=True)
            vin = get_xml_tag_text(result, 'VIN', is_ro=True)
            customer_number = get_xml_tag_text(result, 'CustomerKey', is_ro=True)

            if not ro_number or not vin or not customer_number or customer_number == '0': continue

            unique_vins.add(vin)
            unique_customers.add(customer_number)
            unique_ros.add(ro_number)
            ros.append(xml_to_string(result))

        vehicles, customers, ros_details = execute_ordered_threads(tasks=[
            (self.__get_from_set, unique_vins, "vehicles"),
            (self.__get_from_set, unique_customers, "customers"),
            (self.__get_from_set, unique_ros, "repair_orders")
        ], max_workers=3)

        file_content = {
            "root_repair_orders": ros,
            "vehicles": vehicles,
            "customers": customers,
            "ro_details": ros_details,
            "op_codes": self.op_codes
        }

        self.__upload_to_s3(file_content)