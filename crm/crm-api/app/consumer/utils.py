from crm_orm.models.integration_partner import IntegrationPartner


def get_restricted_query(query, integration_partner):
    """Restrict query based on integration partner."""
    if integration_partner:
        query = query.filter(IntegrationPartner.impel_integration_partner_name == integration_partner)

    return query
