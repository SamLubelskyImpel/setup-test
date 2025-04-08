import logging
from typing import Optional, Dict, List, Tuple

from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger(__name__)

class InvalidFilterException(Exception):
    """Exception raised when a filter key is invalid."""
    pass

class DealerRepository:
    def __init__(self, session):
        self.session = session
        self.filters: List = []

    def create_filters(self, filter_params: dict) -> Optional[Dict[str, str]]:
        """
        Creates filters for querying the database.

        Args:
            filter_params (dict): Dictionary of filter parameters.

        Raises:
            InvalidFilterException: If a key in filter_params does not exist in any model.

        Returns:
            None
        """
        # Reset filters list for every new filter creation
        self.filters = []
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
                self.filters.append(getattr(model, key) == value)
            else:
                logger.warning(f"Invalid key attribute in schema: {key}")
                raise InvalidFilterException(f"Invalid key attribute in schema: {key}")
        return None

    def get_dealers(
        self, 
        filter_params: dict = None, 
        page: int = 1, 
        limit: int = 100
    ) -> Tuple[List[Dealer], bool]:
        """
        Retrieves dealer records with optional filtering and pagination.

        Returns:
            Tuple of (list of Dealer objects, has_next_page flag)
        """
        logger.info(f"[dealers_config] Getting dealers with filters: {filter_params} and pagination: page={page}, limit={limit}")
        query = self.session.query(Dealer)
        if filter_params:
            self.create_filters(filter_params)
            if self.filters:
                query = query.filter(*self.filters)

        query = query.order_by(Dealer.id).offset((page - 1) * limit).limit(limit)

        all_results = query.all()
        has_next_page = len(all_results) == limit

        return all_results, has_next_page

    def create_dealer(self, dealer_data: dict):
        new_dealer = Dealer(
            dealer_name=dealer_data["dealer_name"],
            sfdc_account_id=dealer_data["sfdc_account_id"],
            salesai_dealer_id=dealer_data["salesai_dealer_id"],
            serviceai_dealer_id=dealer_data["serviceai_dealer_id"],
        )
        self.session.add(new_dealer)
        self.session.commit()
        self.session.refresh(new_dealer)
        dealer_id = new_dealer.id

        integration_partner_record = self.session.query(IntegrationPartner.id).filter(
            IntegrationPartner.impel_integration_partner_name == dealer_data["impel_integration_partner_name"]
        ).first()
        if not integration_partner_record:
            raise Exception("Integration partner not found")
        integration_partner_id = integration_partner_record[0]

        new_dealer_integration_partner = DealerIntegrationPartner(
            integration_partner_id=integration_partner_id,
            dealer_id=dealer_id,
            cdp_dealer_id=dealer_data["cdp_dealer_id"],
            is_active=True,
        )
        self.session.add(new_dealer_integration_partner)
        self.session.commit()
        self.session.refresh(new_dealer_integration_partner)

        logger.info(f"Dealer created with ID: {dealer_id} and associated integration partner record.")
        return new_dealer

    def update_dealer(self, dealer_id: int, update_data: dict):
        dealer = self.session.query(Dealer).get(dealer_id)
        if dealer:
            for key, value in update_data.items():
                setattr(dealer, key, value)
            self.session.commit()
            self.session.refresh(dealer)
            return dealer
        raise ValueError("Dealer not found")
