import re
import logging
from os import environ
from datetime import datetime
from shared_class import BaseClass
from adf_template import OEM_MAPPING, OEM_ADF_TEMPLATE

ENVIRONMENT = "test"#environ.get("ENVIRONMENT", "test")
CRM_API_DOMAIN = "crm-api-test.testenv.impel.io"#environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = "impel"#environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

class CRMApiError(Exception):
    pass


class OemAdfCreation(BaseClass):
    """Class for creating an ADF (Automotive Dealership Format) from a lead ID and appointment time if necessary."""
    def __init__(self, oem_recipient) -> None:
        """Initialize API Wrapper."""
        super().__init__()

        self.oem_recipient = oem_recipient
        self.oem_api = self._get_secrets("crm-partner-api", f"{oem_recipient}_oem")

        self.adf_file = OEM_ADF_TEMPLATE
        self.mapper = OEM_MAPPING
        self.formatter = "<{name}>{data}</{name}>\n"

        self.vehicle_of_interest = True

        self.vehicle = ""
        self.customer = ""
        self.vendor = ""

        self.service_code = {
            "acura":{
                "contact": 95081,
                "lead":95079
            },
            "honda": {
                "contact": 90534,
                "lead":90534
            }
        }

    def _generate_vehicle_adf(self, vehicle_of_interest):
        v_status = vehicle_of_interest.get("condition", "ANY")
        v_interest = "buy"
        vehicle_param_status = self.mapper['vehicle']['status'].format(status_value=v_status)
        vehicle_param_interest = self.mapper['vehicle']['interest'].format(interest_value=v_interest)

        default_mapper = {
            'make': self.oem_recipient.upper(),
            'year': datetime.now().year,
            'model': 'Any/All'
        }
        vehicle_data = []
        for key, item in self.mapper['vehicle'].items():
            if key not in ['status', 'interest']:
                default_value = default_mapper.get(key, "")
                value = vehicle_of_interest.get(key) if vehicle_of_interest.get(key) else default_value
                vehicle_data.append(
                    item.format(**{f"{key}_value": value})
                )

        return (
            f"<vehicle{vehicle_param_status}{vehicle_param_interest}>\n"
            + '\n'.join(vehicle_data) + "\n"
            + "</vehicle>"
        )

    def _generate_customer_adf(self, consumer: dict):
        default_mapper = {
            'first_name': "1 Not Available" if self.oem_recipient.lower() == 'honda' else "Anonamous First",
            'last_name': "1 Not Available" if self.oem_recipient.lower() == 'honda' else "Anonamous Last",
            'postalcode': 00000
        }
        consumer_data = []

        for key, item in self.mapper['customer'].items():
            default_value = default_mapper.get(key, "")
            value = consumer.get(key) if consumer.get(key) else default_value
            if key != "address":
                if key == 'phone':
                    digits = re.findall(r'\d', value)
                    if len(digits) >= 10:
                        value = ''.join(digits[-10:])
                consumer_data.append(
                    item.format(**{f"{key}_value": value})
                )
            else:
                for address_key, address_item in item.items():
                    address_data = []
                    address_data.append(
                        address_item.format(**{f"{address_key}_value": value})
                    )
                consumer_data.append(
                    "<address>\n" + '\n'.join(address_data) + "\n</address>"
                )

        return (
            f"<customer>\n<contact>\n"
            + '\n'.join(consumer_data)
            + "\n</contact>\n</customer>"
        )

    def create_adf_data(self, lead_id, dealer_code):
        try:
            vehicle = self.call_crm_api(f"https://{CRM_API_DOMAIN}/leads/{lead_id}")

            vehicle_of_interest = vehicle["vehicles_of_interest"][0] if vehicle.get("vehicles_of_interest", []) else {}
            self.vehicle_of_interest = any(vehicle_of_interest.get(field) is not None for field in ['vin', 'year', 'make', 'model'])
            if self.vehicle_of_interest:
                self.vehicle = self._generate_vehicle_adf(vehicle_of_interest)

            consumer = self.call_crm_api(f"https://{CRM_API_DOMAIN}/consumers/{vehicle.get('consumer_id')}")
            self.customer = self._generate_customer_adf(consumer)

            dealer = self.call_crm_api(f"https://{CRM_API_DOMAIN}/dealers/{consumer.get('dealer_id')}")
            vendor_data = (
                f"{self.mapper['vendor']['id'].format(**{ 'oem_recipient': self.oem_recipient, 'dealer_code': dealer_code })}\n"
                f"{self.mapper['vendor']['vendorname'].format(vendorname_value = dealer.get('dealer_name'))}"
            )
            self.vendor = f"<vendor>\n{vendor_data}\n</vendor>"

            service = "lead" if self.vehicle_of_interest else "contact"

            return self.adf_file.format(
                request_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
                vehicle_of_interest = self.vehicle,
                customer = self.customer,
                vendor = self.vendor,
                service_value = self.service_code[self.oem_recipient][service],
                lead_id = lead_id
            )

        except CRMApiError as e:
            raise e
