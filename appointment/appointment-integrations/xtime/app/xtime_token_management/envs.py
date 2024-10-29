from os import environ


# Environment
LOG_LEVEL = environ.get("LOG_LEVEL", "INFO")
ENV = "prod" if environ.get("ENVIRONMENT", "test").lower() == "prod" else "test"
REGION = environ.get("REGION", "us-east-1")





# Secrets Manager
CRM_INTEGRATION_SECRETS_ID = environ.get(
    "CRM_INTEGRATION_SECRETS_ID",
    f"{'prod' if ENV == 'prod' else 'test'}/xtime_token"
)

