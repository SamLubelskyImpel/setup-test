"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

INVENTORY_DATA_SERVICE_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
INVENTORY_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${InventoryOrderInbound.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

DEALER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${DealerInbound.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

INVENTORY_DATA_SERVICE_INTERPOLATION_DATA = {
    "INVENTORY_DATA_SERVICE_AWS_VALIDATION": INVENTORY_DATA_SERVICE_AWS_VALIDATION,
    "INVENTORY_LAMBDA_INFO": INVENTORY_LAMBDA_INFO,
    "DEALER_LAMBDA_INFO": DEALER_LAMBDA_INFO,
}
