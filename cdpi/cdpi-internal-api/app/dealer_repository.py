import logging
from typing import Optional, Dict, List

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
        self.filters: List = []  # List to hold SQLAlchemy filter expressions

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

    def get_dealers(self, filter_params: dict = None):
        """
        Retrieves dealer records, applying filters if provided.

        Args:
            filter_params (dict, optional): Dictionary of filters to apply.

        Returns:
            List of Dealer objects.
        """
        query = self.session.query(Dealer)
        if filter_params:
            self.create_filters(filter_params)
            if self.filters:
                query = query.filter(*self.filters)
        return query.all()

    def create_dealer(self, dealer_data: dict):
        """
        Creates a new dealer record along with its integration partner record.

        Args:
            dealer_data (dict): Dictionary containing dealer creation data. Expected keys:
                - dealer_name
                - sfdc_account_id
                - salesai_dealer_id
                - serviceai_dealer_id
                - cdp_dealer_id
                - impel_integration_partner_name

        Returns:
            The created Dealer object.
        """
        # Create the new dealer record
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

        # Retrieve the integration partner record by the provided name
        integration_partner_record = self.session.query(IntegrationPartner.id).filter(
            IntegrationPartner.impel_integration_partner_name == dealer_data["impel_integration_partner_name"]
        ).first()
        if not integration_partner_record:
            raise Exception("Integration partner not found")
        integration_partner_id = integration_partner_record[0]

        # Create the DealerIntegrationPartner record using the newly created dealer_id
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
        """
        Updates an existing dealer record.

        Args:
            dealer_id (int): The ID of the dealer to update.
            update_data (dict): Fields to update.

        Returns:
            The updated Dealer object.

        Raises:
            ValueError: If the dealer is not found.
        """
        dealer = self.session.query(Dealer).get(dealer_id)
        if dealer:
            for key, value in update_data.items():
                setattr(dealer, key, value)
            self.session.commit()
            self.session.refresh(dealer)
            return dealer
        raise ValueError("Dealer not found")

    def delete_dealer(self, dealer_id: int):
        """
        Deletes a dealer record.

        Args:
            dealer_id (int): The ID of the dealer to delete.

        Returns:
            True if deletion was successful.

        Raises:
            ValueError: If the dealer is not found.
        """
        dealer = self.session.query(Dealer).get(dealer_id)
        if dealer:
            self.session.delete(dealer)
            self.session.commit()
            return True
        raise ValueError("Dealer not found")
