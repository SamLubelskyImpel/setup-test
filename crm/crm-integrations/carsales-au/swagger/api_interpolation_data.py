"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

CARSALES_AU_WEBHOOK_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
NEW_LEAD_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${NewLeadHandler.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

CARSALES_AU_WEBHOOK_INTERPOLATION_DATA = {
    "CARSALES_AU_WEBHOOK_AWS_VALIDATION": CARSALES_AU_WEBHOOK_AWS_VALIDATION,
    "NEW_LEAD_LAMBDA_INFO": NEW_LEAD_LAMBDA_INFO
}
