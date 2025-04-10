import logging
from typing import Optional, Dict, List, Tuple
from pydantic import BaseModel, Field, ValidationError

from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger(__name__)


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
                logger.debug("Adding filter for %s = %s", key, value)
                self.filters.append(getattr(model, key) == value)
            else:
                logger.warning("Invalid key attribute in schema: %s", key)
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
        logger.info("Getting dealers with filters: %s (page=%s, limit=%s)", filter_params, page, limit)
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
        logger.debug("Query returned %s records. Has next page: %s", len(dealer_records), has_next_page)
        return dealer_records, has_next_page

    def create_dealer(self, dealer_data: dict) -> Dealer:
        """Creates a new dealer and its integration partner record."""
        dealer_create_request = DealerCreateRequest(**dealer_data)

        try:
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
            logger.debug("Dealer record created with id: %s", dealer_id)

            integration_partner_record = self.session.query(IntegrationPartner.id).filter(
                IntegrationPartner.impel_integration_partner_name == dealer_create_request.impel_integration_partner_name
            ).first()
            if not integration_partner_record:
                self.session.rollback()
                logger.error("Integration partner not found for %s", dealer_create_request.impel_integration_partner_name)
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
            logger.info("Dealer and integration partner record created for dealer id: %s", dealer_id)
            return new_dealer
        except ValidationError as e:
            logger.error("Validation error: %s", e, exc_info=True)
            sanitized = self._sanitize_errors(e.errors())
            raise ValidationErrorResponse(sanitized, e)
        except Exception as e:
            self.session.rollback()  # Ensure rollback on any failure
            logger.error("Error creating dealer: %s", str(e), exc_info=True)
            raise

    def update_dealer(self, dealer_id: int, update_data: dict) -> Dealer:
        """Updates an existing dealer record."""
        dealer = self.session.get(Dealer, dealer_id)  # Recommended in SQLAlchemy 1.4+
        if dealer:
            logger.debug("Updating dealer with id: %s", dealer_id)
            for key, value in update_data.items():
                setattr(dealer, key, value)
            self.session.commit()
            self.session.refresh(dealer)
            logger.info("Dealer updated with id: %s", dealer_id)
            return dealer
        logger.error("Dealer with id %s not found", dealer_id)
        raise ValueError("Dealer not found")

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
