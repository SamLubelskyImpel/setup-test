import logging
import boto3
from os import environ
from json import dumps, loads

from appt_orm.session_config import DBSession
from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.op_code_appointment import OpCodeAppointment


SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:143813444726:alert_client_engineering")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
lambda_client = boto3.client("lambda")


def send_opcode_report(report: str) -> None:
    """Send opcode report to CE team."""
    message = f"OpCode Report by Vendor: {report}"
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject='Appointment Service: Dealer OpCode Report',
    )


def get_opcode_mappings(partner_name: str):
    with DBSession() as session:
        dealer_data = session.query(
            Dealer.dealer_name,
            DealerIntegrationPartner.integration_dealer_id,
            OpCode.op_code
        ).join(
            DealerIntegrationPartner, DealerIntegrationPartner.dealer_id == Dealer.id
        ).join(
            IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
        ).join(
            OpCode, DealerIntegrationPartner.id == OpCode.dealer_integration_partner_id
        ).filter(
            IntegrationPartner.impel_integration_partner_name == partner_name,
            DealerIntegrationPartner.is_active == True
        ).all()

        dealer_mappings = {}
        for dealer_name, integration_dealer_id, op_code in dealer_data:
            if integration_dealer_id not in dealer_mappings:
                dealer_mappings[integration_dealer_id] = {
                    "dealer_name": dealer_name,
                    "integration_dealer_id": integration_dealer_id,
                    "op_codes": []
                }
            dealer_mappings[integration_dealer_id]["op_codes"].append(op_code)

        return list(dealer_mappings.values())


def lambda_handler(event, context):
    PARTNERS = ["XTIME"]
    failed_partners = []
    partner_reports = {}
    for partner in PARTNERS:
        logger.info(f"Checking opcodes for partner: {partner}")
        try:
            # Get mapped opcodes
            db_dealers = get_opcode_mappings(partner)
            if not db_dealers:
                continue
            
            logger.info(f"DB Dealers: {db_dealers}")

            integration_dealer_ids = [
                dealer["integration_dealer_id"] for dealer in db_dealers
            ]

            with DBSession() as session:
                partner_metadata = session.query(IntegrationPartner.metadata_).filter(
                    IntegrationPartner.impel_integration_partner_name == partner
                ).first()

            dealer_codes_arn = partner_metadata[0].get("dealer_codes_arn", "")
            if not dealer_codes_arn:
                raise Exception(f"DealerCodes ARN not found in metadata for integration partner {partner}")

            # Get opcodes from XTime
            response = lambda_client.invoke(
                FunctionName=dealer_codes_arn,
                InvocationType="RequestResponse",
                Payload=dumps({"integration_dealer_ids": integration_dealer_ids}),
            )
            logger.info(f"Response from lambda: {response} - {partner}")
            response_json = loads(response["Payload"].read().decode('utf-8'))
            logger.info(f"Response JSON: {response_json}")

            if response_json["statusCode"] != 200:
                raise Exception(f"Failed to get dealer codes for integration partner {partner}")

            # Compare opcodes
            dealer_codes = loads(response_json["body"])["dealer_codes"]

            logger.info("Comparing opcode mappings from DB and Vendor")
            failed_dealers = []
            dealer_reports = {}
            for db_dealer in db_dealers:
                if db_dealer["integration_dealer_id"] not in dealer_codes:
                    failed_dealers.append(db_dealer["dealer_name"])
                    continue

                codes = dealer_codes[db_dealer["integration_dealer_id"]]
                missing_codes = []
                for db_code in db_dealer["op_codes"]:
                    if codes:
                        for code in codes:
                            if str(db_code) == str(code):
                                break
                    else:
                        missing_codes.append(db_code)

                if missing_codes:
                    dealer_reports.update({
                        db_dealer["dealer_name"]: ', '.join(missing_codes)
                    })

            if failed_dealers:
                dealer_reports.update({
                    "Failed to check dealers": ', '.join(failed_dealers)
                })

            if dealer_reports:
                partner_reports.update({
                    partner: dealer_reports
                })

        except Exception as e:
            logger.error(f"Error: {e}")
            failed_partners.append(partner)
            continue

    if not failed_partners and not partner_reports:
        logger.info("No issues found with dealer opcodes")
        return

    report = {}
    if failed_partners:
        report.update({
            "Failed to check vendors": ', '.join(failed_partners)
        })

    if partner_reports:
        report.update({
            "Missing OpCode Mappings": partner_reports
        })

    send_opcode_report(report)
