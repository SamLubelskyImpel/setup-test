import os

import boto3
import pytest
from moto import mock_aws

from access_token.schemas import Token, TekionCredentials
from send_activity.schemas import SendActivityEvent

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def aws_s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])


@pytest.fixture(scope="function")
def aws_sqs(aws_credentials):
    with mock_aws():
        yield boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])

@pytest.fixture(scope="function")
def aws_sns(aws_credentials):
    with mock_aws():
        yield boto3.client("sns", region_name=os.environ["AWS_DEFAULT_REGION"])


@pytest.fixture(scope="function")
def aws_secret_manager(aws_credentials):
    with mock_aws():
        yield boto3.client(
            "secretsmanager", region_name=os.environ["AWS_DEFAULT_REGION"]
        )


@pytest.fixture
def token() -> Token:
    return Token(
        token="fake-token",
        token_type="Bearer",
        expires_in_seconds=3600,
    )


@pytest.fixture
def token_creds() -> TekionCredentials:
    return TekionCredentials(
        url="http://fake-auth-uri.com",
        app_id="fake_app_id",
        secret_key="fake_secret_key",
    )

@pytest.fixture
def send_activity_event():
    return SendActivityEvent(
        lead_id= 0,
        crm_lead_id= "666b4b09f99c554bd1755191",
        dealer_integration_partner_id= 0,
        crm_dealer_id= "techmotors_4_0",
        consumer_id= 0,
        crm_consumer_id= "345787",
        activity_id= 4,
        notes= "View conversation https://assistant.impel.io/conversation?id=aca0bef091f15e4a42c95838552fe6910LxxV&viewonly=true\\n\\nReply as assistant https://assistant.impel.io/conversation?id=aca0bef091f15e4a42c95838552fe6910LxxV\\n\\n\\nStop communication: https://unsubscribe.impel.io/crm/navarre-chevrolet/annalshuford@yahoo.com/aca0bef091f15e4a42c95838552fe6910LxxV\\n*EMAIL*\\n**Requires human takeover**\\nAssistant says:\\nHi Anna,\\n\\nIt\'s Stella Miller from Navarre Chevrolet.\\n\\nThank you for contacting us.\\nI will check this for you soon and a member of our team will get back to you after that.\\n\\nWhat is the best number to reach you?\\n\\n\\nBest Regards,\\n\\nStella Miller\\nAssistant Digital Sales Manager\\n\\nNavarre Chevrolet\\n1300 East College St.\\nLake Charles, Louisiana 70607\\n\\nSales: (337) 485-5385\\nService: (337) 485-5385\\nParts: (337) 485-5385\\n\\n https://www.navarreauto.com \\n\\nTo unsubscribe  click here .\\n\\nClient Says:\\n                    I\'d like to know if the Used 2010 Ford F-150 XLT SuperCrew you have listed on Cars.com for $10,900 is still available.                                                      Give us feedback! Was this lead a quality lead? Yes: https://www.cars.com/leads/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWFkX2lkIjoiMzI5MDVjYTktNDFhOC00MWE2LWI4MWUtMmMyYzFkMzBkYWNmIn0.pJ35RhrMESPzbKl35uTNa_ABxQ0zpAanoFLYWt5xUaI/feedback/?qualityLead=true No: https://www.cars.com/leads/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWFkX2lkIjoiMzI5MDVjYTktNDFhOC00MWE2LWI4MWUtMmMyYzFkMzBkYWNmIn0.pJ35RhrMESPzbKl35uTNa_ABxQ0zpAanoFLYWt5xUaI/feedback/?qualityLead=false\\nBest Time: No Preference\\nPreferred Contact Method: No Preference",
        activity_due_ts= None,
        activity_requested_ts= "2024-02-19T15:59:57Z",
        dealer_timezone= "America/Chicago",
        activity_type= "note",
        contact_method= "email"
    )

