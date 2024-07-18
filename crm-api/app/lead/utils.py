from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner


def get_restricted_query(session, integration_partner):
    query = session.query(Lead).join(Lead.consumer)

    if integration_partner:
        query = (
            query.join(Consumer.dealer_integration_partner)
                 .join(DealerIntegrationPartner.integration_partner)
                 .filter(IntegrationPartner.impel_integration_partner_name == integration_partner)
        )

    return query
