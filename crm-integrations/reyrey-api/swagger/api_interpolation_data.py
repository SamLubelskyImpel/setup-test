"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

REYREY_CRM_API_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  basic:
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: basic
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

UPDATE_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${UpdateLead.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

REYREY_CRM_API_INTERPOLATION_DATA = {
    "REYREY_CRM_API_AWS_VALIDATION": REYREY_CRM_API_AWS_VALIDATION,
    "CREATE_LEAD_LAMBDA_INFO": CREATE_LEAD_LAMBDA_INFO,
    "UPDATE_LEAD_LAMBDA_INFO": UPDATE_LEAD_LAMBDA_INFO,
}
