import logging
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

logger = logging.getLogger(__name__)

@dataclass
class Metadata:
    userId: Optional[str] = None
    adf_email_recipients: List[str] = field(default_factory=list)

@dataclass
class DealerInfo:
    integration_partner_name: str
    product_dealer_id: str
    sfdc_account_id: str
    dealer_name: str
    timezone: str
    dealer_location_name: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    zip_code: Optional[str] = None
    crm_dealer_id: Optional[str] = None
    is_active_salesai: bool = False
    is_active_chatai: bool = False
    metadata: Metadata = field(default_factory=Metadata)

@dataclass
class DealerStatus:
    is_active_salesai: bool
    is_active_chatai: bool
    metadata: Metadata = field(default_factory=Metadata)
    crm_dealer_id: Optional[str] = None

class DatabaseManager:
    def __init__(self):
        self.dt_now = datetime.now(timezone.utc)
        self.filters = []
        self.dealer_records = []

    def create_filters(self, filter_params: dict) -> Optional[Dict[str, str]]:
        """Creates filters for querying the database."""
        for key, value in filter_params.items():
            model = next((m for m in [DealerIntegrationPartner, Dealer, IntegrationPartner] if hasattr(m, key)), None)
            if model:
                self.filters.append(getattr(model, key) == value)
            else:
                logger.warning(f"Invalid key attribute in schema: {key}")
                return {"statusCode": 404, "body": f"Invalid key attribute in schema: {key}"}
        return None

    def get_dealers_config(self) -> List[Dict[str, str]]:
        """Fetches dealer configurations from the database based on filters."""
        with DBSession() as session:
            query = self._build_query(session)
            self.dealer_records = [
                self._build_dealer_record(*res) for res in query.all()
            ]
        return self.dealer_records

    def post_dealers_config(self, dealer_info: DealerInfo) -> Dict[str, str]:
        """Inserts a new dealer configuration into the database."""
        with DBSession() as session:
            if session.query(Dealer).filter_by(product_dealer_id=dealer_info.product_dealer_id).first():
                return {'statusCode': 400, 'body': 'This dealer config already exists'}

            try:
                integration_partner = session.query(IntegrationPartner).filter_by(
                    impel_integration_partner_name=dealer_info.integration_partner_name
                ).first() or IntegrationPartner(impel_integration_partner_name=dealer_info.integration_partner_name)
                
                session.add(integration_partner)
                session.flush()

                dealer = session.query(Dealer).filter_by(
                    product_dealer_id=dealer_info.product_dealer_id,
                    sfdc_account_id=dealer_info.sfdc_account_id
                ).first() or Dealer(
                    product_dealer_id=dealer_info.product_dealer_id,
                    sfdc_account_id=dealer_info.sfdc_account_id,
                    dealer_name=dealer_info.dealer_name,
                    dealer_location_name=dealer_info.dealer_location_name,
                    country=dealer_info.country,
                    state=dealer_info.state,
                    city=dealer_info.city,
                    zip_code=dealer_info.zip_code,
                    metadata_={'timezone': dealer_info.timezone},
                    db_creation_date=self.dt_now,
                    db_update_date=self.dt_now
                )
                
                session.add(dealer)
                session.flush()

                session.add(DealerIntegrationPartner(
                    dealer_id=dealer.id,
                    integration_partner_id=integration_partner.id,
                    crm_dealer_id=dealer_info.crm_dealer_id,
                    is_active_salesai=dealer_info.is_active_salesai,
                    is_active_chatai=dealer_info.is_active_chatai,
                    metadata_=dealer_info.metadata.__dict__,
                    db_creation_date=self.dt_now,
                    db_update_date=self.dt_now
                ))
                session.commit()

                return {'statusCode': 201, 'body': f'Dealer configuration created successfully dealer_id {dealer.id}'}

            except Exception as e:
                logger.error(f"Error saving dealer configuration: {str(e)}")
                session.rollback()
                return {'statusCode': 500, 'body': 'An error occurred while creating the dealer configuration'}

    def put_dealers_config(self, dealer_status: DealerStatus) -> Dict[str, str]:
        """Updates an existing dealer configuration in the database."""
        with DBSession() as session:
            dip = session.query(DealerIntegrationPartner).filter_by(crm_dealer_id=dealer_status.crm_dealer_id).first()
            if dip:
                dip.is_active_salesai = dealer_status.is_active_salesai
                dip.is_active_chatai = dealer_status.is_active_chatai
                dip.metadata_ = dealer_status.metadata.__dict__ if dealer_status.metadata else dip.metadata_
                dip.db_update_date = self.dt_now
                session.commit()
                return {'statusCode': 200, 'body': 'Information updated'}
            return {'statusCode': 404, 'body': 'Dealer configuration not found'}

    def _build_query(self, session: Session):
        """Builds the query to fetch dealer configurations based on the filters."""
        query = session.query(DealerIntegrationPartner, Dealer, IntegrationPartner)\
                       .join(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)\
                       .join(IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id)
        return query.filter(*self.filters) if self.filters else query

    def _build_dealer_record(self, dip_db, dealer_db, ip_db) -> Dict[str, str]:
        """Builds a dictionary representing a dealer record."""
        return {
            "dealer_integration_partner_id": dip_db.id,
            "crm_dealer_id": dip_db.crm_dealer_id,
            "product_dealer_id": dealer_db.product_dealer_id,
            "dealer_name": dealer_db.dealer_name,
            "integration_partner_name": ip_db.impel_integration_partner_name,
            "sfdc_account_id": dealer_db.sfdc_account_id,
            "dealer_location_name": dealer_db.dealer_location_name,
            "country": dealer_db.country,
            "state": dealer_db.state,
            "city": dealer_db.city,
            "zip_code": dealer_db.zip_code,
            "timezone": dealer_db.metadata_.get("timezone") if dealer_db.metadata_ else "",
            "metadata": dip_db.metadata_,
            "is_active_salesai": dip_db.is_active_salesai,
            "is_active_chatai": dip_db.is_active_chatai,
        }
