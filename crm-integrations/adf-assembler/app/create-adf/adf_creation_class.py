import boto3
import logging
import requests
from json import loads
from datetime import datetime
from adf_template import LEAD_DATA_TO_ADF_MAPPER, BASE_ADF_TEMPLATE
from os import environ

ENVIRONMENT = environ.get("ENVIRONMENT")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


class CRMApiError(Exception):
    pass


class AdfCreation:
    """Class for creating an ADF (Automotive Dealership Format) from a lead ID and appointment time if necessary."""

    def __init__(self) -> None:
        """Initialize API Wrapper."""
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self._get_secrets()

        self.adf_file = BASE_ADF_TEMPLATE
        self.mapper = LEAD_DATA_TO_ADF_MAPPER
        self.formatter = "<{name}>{data}</{name}>\n"

        self.vehicle = ""
        self.customer = ""
        self.vendor = ""

    def _get_secrets(self):
        """Retrieve API secret from AWS Secrets Manager."""
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
        )
        secret_data = loads(secret["SecretString"])
        return loads(secret_data[self.partner_id])["api_key"]

    def _create_color_combination(self, color_data):
        """Create color combination data for ADF."""
        color_combination = ""
        for color_type in ["interior_color", "exterior_color"]:
            color = color_data.get(color_type)
            if color:
                color_combination += self.formatter.format(
                    name=color_type.replace("_", ""), data=color
                )

        if color_combination:
            return f"""
                <colorcombination>
                    {color_combination}
                    <preference>1</preference>
                </colorcombination>
            """
        return color_combination


    def _generate_parameter_format(self, key, item, lead_data):
        """Generate parameter format for ADF."""
        param_formatter = "<{name} {param}>{data}</{name}>\n"
        if key == "odometer_units":
            return param_formatter.format(
                name="odometer", 
                param=f'status="unknown" units="{lead_data.get("odometer_units", "miles")}"',
                data=lead_data.get('mileage')
            )
        if key == "price":
            return param_formatter.format(
                name="price", 
                param='type="quote" currency="USD"',
                data=lead_data.get('price')
            )


    def generate_adf_from_lead_data(self, lead_data: dict, lead_category: str = ""):
        """
        Create ADF data based on the provided lead data and category.

        Parameters:
            lead_data (dict): A dictionary containing lead data.
            lead_category (str, optional): Category of the lead. Defaults to "".

        Returns:
            str: A string representing the ADF data.
        """
        category_data = ""

        # Iterate over lead data and format accordingly
        for key, item in lead_data.items():
            if key in self.mapper and item:
                mapper_name = self.mapper[key]
                if mapper_name == "PARAMETERS":
                    category_data += self._generate_parameter_format(key, item, lead_data)
                else:
                    category_data += self.formatter.format(name=mapper_name, data=item)

        # Add color combination data
        category_data += self._create_color_combination(lead_data)

        # Additional formatting for customer category
        if lead_category == "customer":
            name_parts = ("first", "middle", "last")
            for part in name_parts:
                name_value = lead_data.get(part + "_name")
                if name_value:
                    category_data += f'<name part="{part}">{name_value}</name>\n'

        return category_data


    def call_crm_api(self, url):
        """Call CRM API."""
        try:
            response = requests.get(
                url=url,
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error occurred calling CRM API: {e}")
            raise CRMApiError(f"Error occurred calling CRM API: {e}")


    def create_adf_data(self, lead_id, appointment_time=None):
        """
        Creates ADF data from the given lead ID and appointment time if available.

        Parameters:
            lead_id (str): The ID of the lead.
            appointment_time (str, optional): The appointment time. Defaults to None.

        Returns:
            tuple: A tuple containing ADF data and integration partner name.
                The ADF data includes lead ID, request date, vehicle information,
                customer information, and vendor information.
        """
        try:
            vehicle = self.call_crm_api(f"https://{CRM_API_DOMAIN}/leads/{lead_id}")
            lead_comment = vehicle.get("lead_comment")
            self.vehicle = self.generate_adf_from_lead_data(
                vehicle.get("vehicles_of_interest", [""])[0], "vehicle"
            )

            consumer = self.call_crm_api(f"https://{CRM_API_DOMAIN}/consumers/{vehicle.get('consumer_id')}")
            consumer |= {"comment": appointment_time or lead_comment}
            self.customer = self.generate_adf_from_lead_data(consumer, "customer")

            dealer = self.call_crm_api(f"https://{CRM_API_DOMAIN}/dealers/{consumer.get('dealer_id')}")
            self.vendor = self.generate_adf_from_lead_data(dealer, "vendor")
            
            return self.adf_file.format(
                lead_id=lead_id,
                request_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                vehicle=self.vehicle,
                customer=self.customer,
                vendor=self.vendor,
                vendor_full_name=dealer.get("dealer_name"),
            ), dealer.get("integration_partner_name")

        except CRMApiError as e:
            raise e
