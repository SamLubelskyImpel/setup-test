"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

CRM_API_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
CREATE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${CreateLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEADS_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLeads.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEAD_BY_CRM_IDS_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLeadByCrmIds.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEAD_BY_CONSUMER_ID_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLeadByConsumerId.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

UPDATE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${UpdateLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEAD_STATUS_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLeadStatus.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_LEAD_ACTIVITIES_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveLeadActivities.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_SALESPERSONS_DATA_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveSalespersonsData.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

CREATE_CONSUMER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${CreateConsumer.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_CONSUMER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveConsumer.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_CONSUMER_BY_CRM_IDS_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveConsumerByCrmIds.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

UPDATE_CONSUMER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${UpdateConsumer.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_DEALERS_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveDealers.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_DEALER_BY_ID_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveDealerById.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_SALESPERSONS_BY_DEALER_ID_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveSalespersonsByDealerId.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

CREATE_ACTIVITY_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${CreateActivity.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

UPDATE_ACTIVITY_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${UpdateActivity.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_ACTIVITY_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveActivity.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

UPLOAD_DATA_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${UploadData.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

DEALERS_CONFIG_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${DealersConfig.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

DEALER_LEAD_STATUSES_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${GetDealerLeadStatuses.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

CRM_API_INTERPOLATION_DATA = {
    "CRM_API_AWS_VALIDATION": CRM_API_AWS_VALIDATION,
    "CREATE_LEAD_LAMBDA_INFO": CREATE_LEAD_LAMBDA_INFO,
    "RETRIEVE_LEADS_LAMBDA_INFO": RETRIEVE_LEADS_LAMBDA_INFO,
    "RETRIEVE_LEAD_LAMBDA_INFO": RETRIEVE_LEAD_LAMBDA_INFO,
    "RETRIEVE_LEAD_BY_CRM_IDS_LAMBDA_INFO": RETRIEVE_LEAD_BY_CRM_IDS_LAMBDA_INFO,
    "RETRIEVE_LEAD_BY_CONSUMER_ID_LAMBDA_INFO": RETRIEVE_LEAD_BY_CONSUMER_ID_LAMBDA_INFO,
    "UPDATE_LEAD_LAMBDA_INFO": UPDATE_LEAD_LAMBDA_INFO,
    "RETRIEVE_LEAD_STATUS_LAMBDA_INFO": RETRIEVE_LEAD_STATUS_LAMBDA_INFO,
    "RETRIEVE_SALESPERSONS_DATA_LAMBDA_INFO": RETRIEVE_SALESPERSONS_DATA_LAMBDA_INFO,
    "RETRIEVE_LEAD_ACTIVITIES_LAMBDA_INFO": RETRIEVE_LEAD_ACTIVITIES_LAMBDA_INFO,
    "CREATE_CONSUMER_LAMBDA_INFO": CREATE_CONSUMER_LAMBDA_INFO,
    "RETRIEVE_CONSUMER_LAMBDA_INFO": RETRIEVE_CONSUMER_LAMBDA_INFO,
    "RETRIEVE_CONSUMER_BY_CRM_IDS_LAMBDA_INFO": RETRIEVE_CONSUMER_BY_CRM_IDS_LAMBDA_INFO,
    "UPDATE_CONSUMER_LAMBDA_INFO": UPDATE_CONSUMER_LAMBDA_INFO,
    "CREATE_ACTIVITY_LAMBDA_INFO": CREATE_ACTIVITY_LAMBDA_INFO,
    "UPLOAD_DATA_LAMBDA_INFO": UPLOAD_DATA_LAMBDA_INFO,
    "RETRIEVE_DEALERS_LAMBDA_INFO": RETRIEVE_DEALERS_LAMBDA_INFO,
    "RETRIEVE_DEALER_BY_ID_LAMBDA_INFO": RETRIEVE_DEALER_BY_ID_LAMBDA_INFO,
    "RETRIEVE_SALESPERSONS_BY_DEALER_ID_LAMBDA_INFO": RETRIEVE_SALESPERSONS_BY_DEALER_ID_LAMBDA_INFO,
    "UPDATE_ACTIVITY_LAMBDA_INFO": UPDATE_ACTIVITY_LAMBDA_INFO,
    "DEALERS_CONFIG_LAMBDA_INFO": DEALERS_CONFIG_LAMBDA_INFO,
    "RETRIEVE_ACTIVITY_LAMBDA_INFO": RETRIEVE_ACTIVITY_LAMBDA_INFO,
    "DEALER_LEAD_STATUSES_LAMBDA_INFO": DEALER_LEAD_STATUSES_LAMBDA_INFO
}
