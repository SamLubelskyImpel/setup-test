from os import environ


# Environment
LOG_LEVEL = environ.get("LOG_LEVEL", "INFO")
ENV = "prod" if environ.get("ENVIRONMENT", "test").lower() == "prod" else "test"
REGION = environ.get("REGION", "us-east-1")

# Partner
SECRET_KEY = environ.get("SECRET_KEY", "TEKION_V3")
PARTNER_KEY = environ.get("PARTNER_KEY", "TEKION")

# S3
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET", f"crm-integrations-{ENV}")
TOKEN_FILE = environ.get("TOKEN_FILE", f"{PARTNER_KEY.lower()}_crm/token.json")

# Secrets Manager
CRM_INTEGRATION_SECRETS_ID = environ.get(
    "CRM_INTEGRATION_SECRETS_ID",
    f"{'prod' if ENV == 'prod' else 'test'}/crm-integrations-partner"
)

# Tekion API
CRM_TEKION_AUTH_ENDPOINT = environ.get(
    "CRM_TEKION_AUTH_ENDPOINT", "/openapi/public/tokens"
)

# Crm API
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY", "internal_tekion")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN", "crm-api-test.testenv.impel.io")