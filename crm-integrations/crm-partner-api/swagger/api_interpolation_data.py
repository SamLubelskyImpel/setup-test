"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

CRM_PARTNER_API_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  basic:
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: basic
"""

# httpMethod should always be POST even for other endpoint types
REYREY_CREATE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${ReyReyCreateLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

REYREY_UPDATE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${ReyReyUpdateLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

CRM_PARTNER_API_INTERPOLATION_DATA = {
    "CRM_PARTNER_API_AWS_VALIDATION": CRM_PARTNER_API_AWS_VALIDATION,
    "REYREY_CREATE_LEAD_LAMBDA_INFO": REYREY_CREATE_LEAD_LAMBDA_INFO,
    "REYREY_UPDATE_LEAD_LAMBDA_INFO": REYREY_UPDATE_LEAD_LAMBDA_INFO,
}
