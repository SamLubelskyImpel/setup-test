import logging
from os import environ
from sqlalchemy import or_
from sqlalchemy.orm import outerjoin
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel, Field, ValidationError

from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class InvalidFilterException(Exception):
    """Exception raised when a filter key is invalid."""
    pass


class ValidationErrorResponse(Exception):
    def __init__(self, errors, full_errors):
        self.errors = errors
        self.full_errors = full_errors


class DealerCreateRequest(BaseModel):
    dealer_name: str = Field(..., max_length=80, description="Name of the dealer")
    sfdc_account_id: str = Field(..., max_length=40, description="Salesforce Account ID")
    salesai_dealer_id: str = Field(..., max_length=80, description="SalesAI Dealer ID")
    serviceai_dealer_id: str = Field(..., max_length=80, description="ServiceAI Dealer ID")
    cdp_dealer_id: str = Field(..., max_length=80, description="CDP Dealer ID")
    impel_integration_partner_name: str = Field(..., max_length=80, examples=["FORD_DIRECT"])
    is_active: bool = Field(False, description="Is the dealer active?")

class DealerUpdateData(BaseModel):
    dealer_id: int
    dealer_name: Optional[str] = None
    sfdc_account_id: Optional[str] = None
    salesai_dealer_id: Optional[str] = None
    serviceai_dealer_id: Optional[str] = None
    is_active: Optional[bool] = None


class DealerRepository:
    def __init__(self, session):
        self.session = session
        self.filters: List = []

    def create_filters(self, filter_params: dict) -> None:
        """Create SQLAlchemy filter objects for query filtering."""
        self.filters = []  # Reset filters
        for key, value in filter_params.items():
            model = next(
                (
                    m
                    for m in [DealerIntegrationPartner, Dealer, IntegrationPartner]
                    if hasattr(m, key)
                ),
                None,
            )
            if model:
                logger.debug(f"Adding filter for {key} = {value}")
                self.filters.append(getattr(model, key) == value)
            else:
                logger.warning(f"Invalid key attribute in schema: {key}")
                raise InvalidFilterException(f"Invalid key attribute in schema: {key}")

    def get_dealers(
        self, 
        filter_params: dict = None, 
        page: int = 1, 
        limit: int = 100
    ) -> Tuple[List[Dict[str, any]], bool]:
        """
        Retrieves dealer records with optional filtering and pagination.
        
        Returns:
            Tuple of (list of dealer records, has_next_page flag)
        """
        logger.info(f"Getting dealers with filters: {filter_params}\n (page={page}, limit={limit})")
        query = self.session.query(Dealer, DealerIntegrationPartner, IntegrationPartner) \
            .join(DealerIntegrationPartner, Dealer.id == DealerIntegrationPartner.dealer_id) \
            .join(IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id)
        
        if filter_params:
            self.create_filters(filter_params)
            if self.filters:
                query = query.filter(*self.filters)

        query = query.order_by(Dealer.id).offset((page - 1) * limit).limit(limit)
        results = query.all()
        dealer_records = [self._build_dealer_record(*res) for res in results]
        has_next_page = len(dealer_records) == limit
        logger.debug(f"Query returned {len(dealer_records)} records. Has next page: {has_next_page}")
        return dealer_records, has_next_page

    def create_dealer(self, dealer_data: dict) -> Dealer:
        """Creates a new dealer and its integration partner record."""
        try:
            dealer_create_request = DealerCreateRequest(**dealer_data)
            
            existing_dealer = self.session.query(Dealer) \
                .outerjoin(DealerIntegrationPartner, Dealer.id == DealerIntegrationPartner.dealer_id) \
                .filter(
                    or_(
                        Dealer.sfdc_account_id == dealer_create_request.sfdc_account_id,
                        DealerIntegrationPartner.cdp_dealer_id == dealer_create_request.cdp_dealer_id
                    )
                ).first()

            if existing_dealer:
                logger.info(
                    "Duplicate dealer found: sfdc_account_id=%s or cdp_dealer_id=%s",
                    dealer_create_request.sfdc_account_id,
                    dealer_create_request.cdp_dealer_id
                )
                raise Exception("Duplicate dealer exists based on sfdc_account_id or cdp_dealer_id")

            new_dealer = Dealer(
                dealer_name=dealer_create_request.dealer_name,
                sfdc_account_id=dealer_create_request.sfdc_account_id,
                salesai_dealer_id=dealer_create_request.salesai_dealer_id,
                serviceai_dealer_id=dealer_create_request.serviceai_dealer_id,
            )
            self.session.add(new_dealer)
            self.session.commit()
            self.session.refresh(new_dealer)
            dealer_id = new_dealer.id
            logger.debug(f"Dealer record created with id: {dealer_id}")

            integration_partner_record = self.session.query(IntegrationPartner.id).filter(
                IntegrationPartner.impel_integration_partner_name == dealer_create_request.impel_integration_partner_name
            ).first()
            if not integration_partner_record:
                self.session.rollback()
                logger.error(f"Integration partner not found for {dealer_create_request.impel_integration_partner_name}")
                raise Exception("Integration partner not found")
            integration_partner_id = integration_partner_record[0]

            new_dealer_integration_partner = DealerIntegrationPartner(
                integration_partner_id=integration_partner_id,
                dealer_id=dealer_id,
                cdp_dealer_id=dealer_create_request.cdp_dealer_id,
                is_active=dealer_create_request.is_active,
            )
            self.session.add(new_dealer_integration_partner)
            self.session.commit()
            self.session.refresh(new_dealer_integration_partner)
            logger.info(f"Dealer and integration partner record created for dealer id: {dealer_id}")
            return new_dealer
        except ValidationError as e:
            logger.error("Validation error: %s", str(e), exc_info=True)
            sanitized = self._sanitize_errors(e.errors())
            raise ValidationErrorResponse(sanitized, e)
        except Exception as e:
            self.session.rollback()  # Ensure rollback on any failure
            logger.error("Error creating dealer: %s", str(e), exc_info=True)
            raise

    def update_dealer(self, dealer_id: int, update_data: dict) -> Dealer:
        """Updates an existing dealer record."""

        try:
            update_payload = DealerUpdateData(**update_data)

            dealer = self.session.get(Dealer, dealer_id)
            if not dealer:
                logger.error(f"Dealer with id {dealer_id} not found")
                raise ValueError("Dealer not found")

            updated = False  # Flag to track if any change is applied

            dealer_fields = ['dealer_name', 'sfdc_account_id', 'salesai_dealer_id', 'serviceai_dealer_id']
            for field in dealer_fields:
                new_value = getattr(update_payload, field)
                if new_value is not None:
                    setattr(dealer, field, new_value)
                    updated = True

            if update_payload.is_active is not None:
                dealer_integration_partner = self.session.query(DealerIntegrationPartner).filter_by(dealer_id=dealer_id).first()
                if dealer_integration_partner:
                    dealer_integration_partner.is_active = update_payload.is_active
                    updated = True
                else:
                    logger.warning("No DealerIntegrationPartner record found for dealer_id %s", dealer_id)

            if updated:
                self.session.commit()
                self.session.refresh(dealer)
                logger.info("Dealer updated successfully with id: %s", dealer_id)
            else:
                logger.info("No updates were applied for dealer id: %s", dealer_id)

            return dealer, updated
        
        except ValidationError as e:
            logger.error("Validation error: %s", str(e), exc_info=True)
            sanitized = self._sanitize_errors(e.errors())
            raise ValidationErrorResponse(sanitized, e)
        except Exception as e:
            self.session.rollback()
            logger.error("Error updating dealer: %s", str(e), exc_info=True)
            raise

    @staticmethod
    def _build_dealer_record(dealer, dealer_integration_partner, integration_partner) -> Dict[str, any]:
        """Builds a dictionary representing a dealer record."""
        return {
            "impel_integration_partner_name": integration_partner.impel_integration_partner_name,
            "dealer_name": dealer.dealer_name,
            "dealer_id": dealer.id,
            "salesai_dealer_id": dealer.salesai_dealer_id,
            "serviceai_dealer_id": dealer.serviceai_dealer_id,
            "sfdc_account_id": dealer.sfdc_account_id,
            "is_active": dealer_integration_partner.is_active
        }

    @staticmethod
    def _sanitize_errors(errors) -> List[Dict[str, str]]:
        """Simplifies Pydantic validation errors for user consumption."""
        sanitized = []
        for error in errors:
            field = ".".join(map(str, error.get("loc", [])))
            message = error.get("msg", "Invalid input")
            sanitized.append({"field": field, "message": message})
        return sanitized
