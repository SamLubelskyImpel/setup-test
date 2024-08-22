"""Retrieve dealer configs."""
import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def build_dealer_record(dip_db, dealer_db, ip_db) -> dict:
    """Build a dealer record dictionary from database results."""
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
        "is_active": dip_db.is_active,
        "is_active_salesai": dip_db.is_active_salesai,
        "is_active_chatai": dip_db.is_active_chatai,
    }


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve dealer configs."""
    logger.info(f"Event: {event}")

    try:
        dealer_records = []
        filters = []
        query_params = event.get("queryStringParameters", {})

        with DBSession() as session:
            query = session.query(
                DealerIntegrationPartner, Dealer, IntegrationPartner
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            )

            # Dynamic filtering based on provided query parameters
            for key, value in query_params.items():
                if 'date' in key:
                    logger.warning(f"Date filtering requested but not implemented: {key} = {value}")
                    return {
                        "statusCode": 400,
                        "body": dumps({"error": "Filter by date is not implemented"})
                    }
                if hasattr(DealerIntegrationPartner, key):
                    filters.append(getattr(DealerIntegrationPartner, key) == value)
                elif hasattr(Dealer, key):
                    filters.append(getattr(Dealer, key) == value)
                elif hasattr(IntegrationPartner, key):
                    filters.append(getattr(IntegrationPartner, key) == value)
                else:
                    logger.warning(f"Invalid filter key: {key}")
                    return {
                        "statusCode": 404,
                        "body": dumps({"error": f"Invalid key attribute in schema: {key}"})
                    }

            if filters:
                logger.info(f"Applying filters: {filters}")
                query = query.filter(*filters)

            db_results = query.all()
            logger.info(f"Query returned {len(db_results)} results")

            for dip_db, dealer_db, ip_db in db_results:
                dealer_records.append(build_dealer_record(dip_db, dealer_db, ip_db))

        return {
            "statusCode": 200,
            "body": dumps(dealer_records)
        }
    except Exception as e:
        logger.error(f"Error retrieving dealers: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }

# if __name__ == '__main__':
#     response = lambda_handler(event={
#             "queryStringParameters":{
#             }
#         }, context="")
    
#     print(len(response))
#     print(response)
    