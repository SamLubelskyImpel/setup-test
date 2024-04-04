"""Retrieve salespersons by dealer_id."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve dealer by id."""
    logger.info(f"Event: {event}")

    try:
        salespersons_list = []
        product_dealer_id = event["pathParameters"]["dealer_id"]

        with DBSession() as session:
            dealer_salespersons = (
                session.query(
                    Salesperson.last_name,
                    Salesperson.first_name,
                    Salesperson.email,
                    Salesperson.phone,
                    Salesperson.crm_salesperson_id,
                )
                .join(
                    DealerIntegrationPartner,
                    DealerIntegrationPartner.id
                    == Salesperson.dealer_integration_partner_id,
                )
                .join(Dealer, Dealer.id == DealerIntegrationPartner.dealer_id)
                .filter(Dealer.product_dealer_id == product_dealer_id)
                .all()
            )

            if not dealer_salespersons:
                logger.error(f"No salespersons found with dealer_id: {product_dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No dealer found with dealer_id: {product_dealer_id}"})
                }              

            logger.info(f"Found dealer salespersons: {len(dealer_salespersons)}")            

            for salesperson in dealer_salespersons:
                salespersons_list.append(
                    {
                        "Emails": salesperson.email,
                        "FirstName": salesperson.first_name,
                        "FullName": f"{salesperson.first_name} {salesperson.last_name}",
                        "LastName": salesperson.last_name,
                        "Phones": salesperson.phone,
                        "UserId": salesperson.crm_salesperson_id,
                    }
                )

        return {
            "statusCode": 200,
            "body": dumps(salespersons_list),
        }

    except Exception as e:
        logger.error(f"Error retrieving dealer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }

