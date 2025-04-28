from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import logging
from os import environ

from dateutil.relativedelta import relativedelta

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def optin_flag(value):
    """Parse optin flag to boolean."""
    return value == 'Y'


def is_valid_xml(xml_string):
    """Check if xml string is valid."""
    try:
        ET.fromstring(xml_string)
        return True
    except ET.ParseError as e:
        return False


def parse_datetime(datetime_str, parse_format, return_format):
    """Parse datetime string to datetime object."""
    try:
        if datetime_str is None or datetime_str == '0':
            return None
        parsed_datetime = datetime.strptime(datetime_str, parse_format)
        return parsed_datetime.strftime(return_format)
    except (ValueError, TypeError) as e:
        logger.exception(f"Unable to parse datetime: {datetime_str}")
        raise e


def parse_float(float_str):
    """Parse float string to float object."""
    try:
        if float_str is None:
            return None
        return float(float_str)
    except (ValueError, TypeError) as e:
        logger.exception(f"Unable to parse float: {float_str}")
        raise e


def parse_int(int_str):
    """Parse int string to int object."""
    try:
        if int_str is None:
            return None
        return int(int_str)
    except (ValueError, TypeError) as e:
        logger.exception(f"Unable to parse int: {int_str}")
        raise e


def parse_date(date_str):
    """Parse date string to date object."""
    try:
        if date_str is None or date_str == '0':
            return None
        return datetime.strptime(date_str, '%Y%m%d')
    except (ValueError, TypeError) as e:
        logger.exception(f"Unable to parse date: {date_str}")
        raise e


def parse_consumers(customers):
    """Parse consumer xml data to unified format."""
    try:
        parsed_consumers = {}
        for customer_id, customer_xml in customers.items():
            if not is_valid_xml(customer_xml):
                logger.error(f"[SUPPORT ALERT] Unable to parse consumer [CONTENT] Invalid XML for customer ID: {customer_id}")
                parsed_consumers[customer_id] = {}
                continue

            root = ET.fromstring(customer_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com/transitional'}

            consumer = {
                "dealer_customer_no": root.find('ns0:CustomerNumber', ns0).text,
                "first_name": root.find('ns0:FirstName', ns0).text,
                "last_name": root.find('ns0:LastName', ns0).text,
                "email": root.find('ns0:Email1', ns0).text,
                "cell_phone": root.find('ns0:CellPhone', ns0).text,
                "home_phone": root.find('ns0:PhoneNumber', ns0).text,
                "city": root.find('ns0:City', ns0).text,
                "state": root.find('ns0:StateCode', ns0).text,
                "postal_code": root.find('ns0:ZipCode', ns0).text,
                "address": root.find('ns0:Address1', ns0).text,
                "email_optin_flag": optin_flag(root.find('ns0:AllowContactByEmail', ns0).text),
                "phone_optin_flag": optin_flag(root.find('ns0:AllowContactByPhone', ns0).text),
                "postal_mail_optin_flag": optin_flag(root.find('ns0:AllowContactByPostal', ns0).text),
                "sms_optin_flag": optin_flag(root.find('ns0:AllowContactByPhone', ns0).text)
            }

            parsed_consumers[customer_id] = consumer
        return parsed_consumers
    except Exception as e:
        logger.exception("Unable to parse customers")
        raise e


def parse_vehicles(vehicles):
    """Parse vehicle xml data to unified format."""
    try:
        parsed_vehicles = {}
        for vehicle_id, vehicle_xml in vehicles.items():
            if not is_valid_xml(vehicle_xml):
                logger.error(f"[SUPPORT ALERT] Unable to parse vehicle [CONTENT] Invalid XML for vehicle ID: {vehicle_id}")
                parsed_vehicles[vehicle_id] = {}
                continue

            root = ET.fromstring(vehicle_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com/transitional'}

            vehicle = {
                "vin": root.find('ns0:VIN', ns0).text,
                "oem_name": root.find('ns0:Make', ns0).text,
                "make": root.find('ns0:Make', ns0).text,
                "model": root.find('ns0:Model', ns0).text,
                "year": parse_int(root.find('ns0:ModelYear', ns0).text),
                "type": root.find('ns0:BodyStyle', ns0).text,
                "mileage": parse_int(root.find('ns0:Odometer', ns0).text),
                "new_or_used": root.find('ns0:TypeNU', ns0).text,
                "stock_num": root.find('ns0:StockNumber', ns0).text,
                "warranty_expiration_miles": parse_int(root.find('ns0:WarrantyMiles', ns0).text),
                "exterior_color": root.find('ns0:Color', ns0).text,
                "trim": root.find('ns0:Trim', ns0).text
            }

            date_in_service = parse_date(root.find('ns0:DateInService', ns0).text)
            warranty_months = parse_int(root.find('ns0:WarrantyMonths', ns0).text)
            if date_in_service and warranty_months is not None:
                warranty_expiration_date = date_in_service + relativedelta(months=warranty_months)
                vehicle["warranty_expiration_date"] = warranty_expiration_date.strftime('%Y-%m-%d')
            else:
                vehicle["warranty_expiration_date"] = None

            parsed_vehicles[vehicle_id] = vehicle

        return parsed_vehicles
    except Exception as e:
        logger.exception("Unable to parse vehicles")
        raise e


def parse_op_codes(op_codes):
    try:
        parsed_op_codes = {}

        for op_code_xml in op_codes:
            if not is_valid_xml(op_code_xml):
                logger.error(f"[SUPPORT ALERT] Unable to parse op code [CONTENT] Invalid XML for op code: {op_code_xml}")
                continue

            root = ET.fromstring(op_code_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com/transitional'}

            op_code = root.find('ns0:OperationCode', ns0).text
            op_code_desc = ' '.join([root.find('ns0:Description1', ns0).text or '',
                                     root.find('ns0:Description2', ns0).text or '',
                                     root.find('ns0:Description3', ns0).text or '',
                                     root.find('ns0:Description4', ns0).text or ''])
            parsed_op_codes[op_code] = op_code_desc.strip()[:305]

        return parsed_op_codes
    except Exception:
        logger.exception("Unable to parse op codes")
        raise
