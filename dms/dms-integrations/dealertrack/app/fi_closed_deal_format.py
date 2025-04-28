"""Format Dealertrack xml data to unified format."""
import logging
import urllib.parse
import xml.etree.ElementTree as ET
from json import dumps, loads
from os import environ
from typing import Any

import boto3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

from unified_df import upload_unified_json
from format_utils import parse_consumers, parse_vehicles, parse_datetime, parse_float, parse_int, parse_date

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
s3_client = boto3.client("s3")


def parse_xml_to_entries(deal_json, s3_uri):
    """Format dealertrack vehicle sale xml data to unified format."""
    try:
        entries = []

        # s3_uri structure: dealertrack-dms/raw/{resource}/{dealer_id}/{year}/{month}/{day}/{timestamp}_{resource}.json
        dms_id = s3_uri.split('/')[3]

        db_metadata = {
            "Region": REGION,
            "PartitionYear": s3_uri.split("/")[4],
            "PartitionMonth": s3_uri.split("/")[5],
            "PartitionDate": s3_uri.split("/")[6],
            "s3_url": s3_uri,
        }

        # Parse customers
        parsed_consumers = parse_consumers(deal_json.get('customers'))
        parsed_vehicles = parse_vehicles(deal_json.get('vehicles'))

        for deal_xml in deal_json.get('root_deals'):
            root = ET.fromstring(deal_xml)
            ns0 = {'ns0': 'opentrack.dealertrack.com/transitional'}

            db_dealer_integration_partner = {"dms_id": dms_id}
            db_deal = {}
            db_vehicle = parsed_vehicles.get(root.find('ns0:VIN', ns0).text)
            db_consumer = parsed_consumers.get(root.find('ns0:BuyerCustomerNumber', ns0).text)
            db_service_contracts = []

            details_xml = deal_json.get('deals_details').get(root.find('ns0:DealNumber', ns0).text)
            details_root = ET.fromstring(details_xml)

            db_cobuyer = parsed_consumers.get(details_root.find('ns0:CoBuyerNumber', ns0).text)

            # Parse service contracts
            service_contracts = details_root.findall('.//ns0:ServiceContract', ns0)

            for service_contract in service_contracts:
                db_service_contract = {
                    "service_contracts|contract_id": service_contract.find('ns0:ContractNumber', ns0).text,
                    "service_contracts|contract_name": service_contract.find('ns0:ContractName', ns0).text,
                    "service_contracts|start_date": parse_datetime(
                        service_contract.find('ns0:ContractStartDate', ns0).text,
                        parse_format='%Y%m%d',
                        return_format='%Y-%m-%d %H:%M:%S'
                    ),
                    "service_contracts|amount": service_contract.find('ns0:ContractAmount', ns0).text,
                    "service_contracts|cost": service_contract.find('ns0:ContractCost', ns0).text,
                    "service_contracts|deductible": service_contract.find('ns0:ContractDeductible', ns0).text,
                    "service_contracts|expiration_months": service_contract.find('ns0:ContractExpirationMonths', ns0).text,
                    "service_contracts|expiration_miles": service_contract.find('ns0:ContractExpirationMiles', ns0).text
                }
                db_service_contracts.append(db_service_contract)


            # Parse Salespersons

            salespersons = details_root.findall('.//ns0:SalesPerson', ns0)
            first_salesperson = salespersons[0] if salespersons else None
            salesperson_name = None
            salesperson_id = None
            if first_salesperson:
                salesperson_name = first_salesperson.find('ns0:SalesPersonName', ns0).text
                salesperson_id = first_salesperson.find('ns0:SalesPersonID', ns0).text

            # Parse Trade-Ins

            tradeins = details_root.findall('.//ns0:TradeIn', ns0)
            first_tradein = tradeins[0] if tradeins else None

            db_tradein = None

            if first_tradein:
                db_tradein = {
                "vin": first_tradein.find('ns0:VIN', ns0).text,
                "make": first_tradein.find('ns0:TradeMake', ns0).text,
                "model": first_tradein.find('ns0:TradeModel', ns0).text,
                "year": parse_int(first_tradein.find('ns0:TradeYear', ns0).text),
                "mileage": parse_int(first_tradein.find('ns0:Odometer', ns0).text)
            }

            # Complex fields

            sale_type = details_root.find('ns0:RecordType', ns0).text
            is_lease = sale_type == 'L'

            finance_term = details_root.find('ns0:LeaseTerm', ns0).text if is_lease else details_root.find('ns0:RetailTerm', ns0).text

            lease_mileage_limit = parse_int(details_root.find('ns0:MilesYearAllow', ns0).text) * (parse_int(details_root.find('ns0:LeaseTerm', ns0).text) / 12)

            payment_frequency = details_root.find('ns0:LeaseTerm', ns0).text

            montly_payment_amount = parse_float(details_root.find('ns0:FinancedPayment', ns0).text)

            if payment_frequency == 'W': #Weekly
                montly_payment_amount *= 4
            elif payment_frequency == 'B': #Biweekly
                montly_payment_amount *= 2
            elif payment_frequency == 'S': #Semi Monthly
                montly_payment_amount *= 2
            elif payment_frequency == 'Q': #Quarterly
                montly_payment_amount /= 3
            elif payment_frequency == 'L': #Semi Annual
                montly_payment_amount /= 6
            elif payment_frequency == 'A': #Annual
                montly_payment_amount /= 12

            # Parse deal data
            db_deal = {
                "sale_date": parse_datetime(
                    root.find('ns0:DealDate', ns0).text,
                    parse_format='%Y%m%d',
                    return_format='%Y-%m-%d %H:%M:%S'
                ),
                "listed_price": parse_float(details_root.find('ns0:RetailPrice', ns0).text),
                "sales_tax": parse_float(details_root.find('ns0:Tax1', ns0).text),
                "mileage_on_vehicle": parse_int(details_root.find('ns0:OdometerAtSale', ns0).text),
                "deal_type": details_root.find('ns0:SaleType', ns0).text,
                "sale_type": sale_type,
                "cost_of_vehicle": parse_float(details_root.find('ns0:VehicleCost', ns0).text),
                "oem_msrp": parse_float(details_root.find('ns0:MSRP', ns0).text),
                "adjustment_on_price": parse_float(details_root.find('ns0:AdjCapitalizedCost', ns0).text),
                "trade_in_value": parse_float(details_root.find('ns0:TradeAllowance', ns0).text),
                "payoff_on_trade": parse_float(details_root.find('ns0:TradePayoff', ns0).text),
                "miles_per_year": parse_int(details_root.find('ns0:MilesYearAllow', ns0).text),
                "profit_on_sale": parse_float(details_root.find('ns0:CommissionableGross', ns0).text),
                "has_service_contract": bool(db_service_contracts),
                "vehicle_gross": parse_float(details_root.find('ns0:RetailPrice', ns0).text),
                "vin": root.find('ns0:VIN', ns0).text,
                "finance_rate": details_root.find('ns0:APR', ns0).text,
                "finance_term": finance_term,
                "finance_amount": details_root.find('ns0:AmountFinanced', ns0).text,
                "transaction_id": details_root.find('ns0:DealNumber', ns0).text,
                "first_payment": parse_datetime(details_root.find('ns0:DateFirstPayment', ns0).text, parse_format='%Y%m%d',
                    return_format='%Y-%m-%d'),
                "residual_value": parse_float(details_root.find('ns0:ResidualAmount', ns0).text),
                "monthly_payment_amount": montly_payment_amount,
                "lease_mileage_limit": lease_mileage_limit,
                "expected_payoff_date": parse_datetime(details_root.find('ns0:DateLastPayment', ns0).text, parse_format='%Y%m%d',
                    return_format='%Y-%m-%d'),
                "assignee_dms_id": salesperson_id,
                "assignee_name": salesperson_name
            }

            metadata = dumps(db_metadata)
            db_vehicle["metadata"] = metadata
            db_consumer["metadata"] = metadata
            db_deal["metadata"] = metadata

            entry = {
                "dealer_integration_partner": db_dealer_integration_partner,
                "vehicle_sale": db_deal,
                "vehicle": db_vehicle,
                "consumer": db_consumer,
                "service_contracts.service_contracts": db_service_contracts,
                "trade_in_vehicle": db_tradein,
                "cobuyer_consumer": db_cobuyer,
            }

            entries.append(entry)

        logger.info("Inserting entries: %s", entries)

        return entries, dms_id
    except Exception as e:
        logger.error("Unable to parse vehicle sale data")
        raise e


def record_handler(record: SQSRecord) -> None:
    """Transform DealerTrack vehicle sale record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        bucket = body['Records'][0]['s3']['bucket']['name']
        key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(key)
        response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        content = response['Body'].read().decode('utf-8')
        deal_json = loads(content)
        entries, dms_id = parse_xml_to_entries(deal_json, decoded_key)
        upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming Dealertrack fi closed deal record: {record}")
        raise

def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealertrack data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
