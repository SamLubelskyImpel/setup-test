import logging
from typing import List, Optional, Dict
from dataclasses import dataclass
from datetime import datetime, timezone
from utils import is_valid_timezone
from sqlalchemy.orm import Session

from appt_orm.session_config import DBSession
from appt_orm.models.dealer import Dealer
from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.product import Product
from appt_orm.models.op_code import OpCode
from appt_orm.models.service_type import ServiceType

logger = logging.getLogger(__name__)

class InvalidFilterException(Exception):
    """Custom exception for invalid filters."""
    pass


@dataclass
class DealerOnboardingInfo:
    integration_partner_name: str
    integration_op_code: str
    integration_op_code_description: str
    product_name: str
    dealer_name: str
    timezone: str
    sfdc_account_id: str
    is_active: bool = False
    service_type: str
    integration_dealer_id: str
    dealer_location_name: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    zip_code: Optional[str] = None


class DatabaseManager:
    def __init__(self):
        self.dt_now = datetime.now(timezone.utc)
        self.filters = []
        self.dealer_records = []

    def create_filters(self, filter_params: dict) -> Optional[Dict[str, str]]:
        """Creates filters for querying the database."""

        valid_fields = {}
        for model in [DealerIntegrationPartner, Dealer, IntegrationPartner, Product]:
            valid_fields.update({attr: model for attr in model.__table__.columns.keys()})
        
        invalid_keys = []
        for key, value in filter_params.items():
            model = valid_fields.get(key)
            if model:
                self.filters.append(getattr(model, key) == value)
            else:
                invalid_keys.append(key)

        if invalid_keys:
            error_message = f"Invalid key attribute(s) in schema: {', '.join(invalid_keys)}"
            logger.warning(error_message)
            raise InvalidFilterException(error_message)

        return None
    

    def get_dealer_info(self) -> List[Dict[str, str]]:
        """Fetches dealers onboarded from the database based on filters."""
        with DBSession() as session:
            query = self._build_query(session)
            self.dealer_records = [
                self._build_dealer_record(*res) for res in query.all()
            ]
        return self.dealer_records

    def post_dealer_onboarding_info(self, dealer_info: DealerOnboardingInfo) -> Dict[str, str]:
        """Inserts into the database a new appt_dealer, appt_dealer_integration_partner and appt_op_code based on the DealerOnboardingInfo."""

        if not is_valid_timezone(dealer_info.timezone):
            logger.warning(f"Invalid timezone: {dealer_info.timezone} - Dealer Onboarding Failed")
            return {"statusCode": 400, "body": "Invalid timezone - Dealer Onboarding Failed"}
        
        with DBSession() as session:
            logger.info("Checking existing integration_partner")

            integration_partner = (
                session.query(IntegrationPartner)
                .filter_by(impel_integration_partner_name=dealer_info.integration_partner_name)
                .first()
            )

            if not integration_partner:
                logger.warning(f"Integration Partner '{dealer_info.integration_partner_name}' not found")
                return {
                    "statusCode": 404,
                    "body": f"Integration Partner '{dealer_info.integration_partner_name}' not found",
                }

            logger.info("Checking existing product")

            product = (
                session.query(Product)
                .filter_by(product_name=dealer_info.product_name)
                .first()
            )

            if not product:
                logger.warning(f"Product '{dealer_info.product_name}' not found")
                return {
                    "statusCode": 404,
                    "body": f"Product '{dealer_info.product_name}' not found",
                }

            logger.info("Checking existing service_type")

            service_type = (
                session.query(ServiceType)
                .filter_by(service_type=dealer_info.service_type)
                .first()
            )

            if not service_type:
                logger.warning(f"Service Type '{dealer_info.service_type}' not found")
                return {
                    "statusCode": 404,
                    "body": f"Service Type '{dealer_info.service_type}' not found",
                }
            

            try:

                logger.info(f"Checking existing dealer")

                dealer = (
                    session.query(Dealer)
                    .filter(Dealer.sfdc_account_id == dealer_info.sfdc_account_id)
                    .first()
                )

                if dealer:
                    logger.warning(f"Dealer with sfdc_account_id '{dealer_info.sfdc_account_id}' already exists")

                else:
                    logger.info("Creating new dealer")

                    dealer = Dealer(
                        sfdc_account_id=dealer_info.sfdc_account_id,
                        dealer_name=dealer_info.dealer_name,
                        timezone=dealer_info.timezone,
                        dealer_location_name=dealer_info.dealer_location_name,
                        country=dealer_info.country,
                        state=dealer_info.state,
                        city=dealer_info.city,
                        zip_code=dealer_info.zip_code
                    )

                    session.add(dealer)
                    session.flush()

                logger.info("Checking existing dealer_integration_partner")

                dealer_integration_partner = (
                    session.query(DealerIntegrationPartner)
                    .filter(DealerIntegrationPartner.integration_partner_id == integration_partner.id)
                    .filter(DealerIntegrationPartner.dealer_id == dealer.id)
                    .filter(DealerIntegrationPartner.product_id == product.id)
                    .first()
                )

                if dealer_integration_partner:
                    logger.warning(f"Dealer Integration Partner already exists - dip_id: {dealer_integration_partner.id}")
                    return {
                        "statusCode": 409,
                        "body": f"Dealer Integration Partner already exists - dip_id: {dealer_integration_partner.id}",
                    }
                
                else:
                    
                    logger.info("Creating new dealer_integration_partner")

                    dealer_integration_partner = DealerIntegrationPartner(
                        integration_dealer_id=dealer_info.integration_dealer_id,
                        integration_partner_id=integration_partner.id,
                        dealer_id=dealer.id,
                        product_id=product.id,
                        is_active=dealer_info.is_active
                    )

                    session.add(dealer_integration_partner)
                    session.flush()

                logger.info("Creating new op_code")

                op_code = OpCode(
                    dealer_integration_partner_id=dealer_integration_partner.id,
                    op_code=dealer_info.integration_op_code,
                    op_code_description=dealer_info.integration_op_code_description,
                    service_type_id=service_type.id
                )

                session.add(op_code)
                session.commit()

                logger.info(f"Dealer onboarded successfully - dealer_id: {dealer.id} - dip_id: {dealer_integration_partner.id}")

                return {
                    "statusCode": 201,
                    "body": f"Dealer onboarded successfully - dealer_id: {dealer.id} - dip_id: {dealer_integration_partner.id}",
                }

            except Exception as e:
                logger.error(f"Error onboarding dealer: {str(e)}")
                session.rollback()
                return {
                    "statusCode": 500,
                    "body": "An error occurred while onboarding the dealer",
                }

    def _build_query(self, session: Session):
        """Builds the query to fetch dealer configurations based on the filters."""
        query = (
            session.query(DealerIntegrationPartner, Dealer, IntegrationPartner, Product, ServiceType, OpCode)
            .join(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
            .join(IntegrationPartner,DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id)
            .join(Product, DealerIntegrationPartner.product_id == Product.id)
            .join(OpCode, OpCode.dealer_integration_partner_id == DealerIntegrationPartner.id)
            .join(ServiceType, ServiceType.id == OpCode.service_type_id)
        )
        return query.filter(*self.filters) if self.filters else query

    def _build_dealer_record(self, dip_db, dealer_db, ip_db, product_db, service_type_db, op_code_db) -> Dict[str, str]:
        """Builds a dictionary representing a dealer record."""

        return {
            "integration_dealer_id": dip_db.integration_dealer_id,
            "dealer_integration_partner_id": dip_db.id,
            "integration_partner_name": ip_db.impel_integration_partner_name,
            "product_name": product_db.product_name,
            "dealer_name": dealer_db.dealer_name,
            "sfdc_account_id": dealer_db.sfdc_account_id,
            "dealer_location_name": dealer_db.dealer_location_name,
            "country": dealer_db.country,
            "state": dealer_db.state,
            "city": dealer_db.city,
            "zip_code": dealer_db.zip_code,
            "timezone": dealer_db.timezone,
            "is_active": dip_db.is_active,
            "integration_op_codes": [
                {
                    "op_code": op_code_db.op_code,
                    "op_code_description": op_code_db.op_code_description,
                    "service_type": service_type_db.service_type
                }
            ]
        }