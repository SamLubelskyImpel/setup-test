"""Contains interpolation data for the swagger api template."""

INVENTORY_INTERNAL_API_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
RETRIEVE_INVENTORY_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveInventory.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_DEALER_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveDealer.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

INVENTORY_INTERNAL_API_INTERPOLATION_DATA = {
    "INVENTORY_INTERNAL_API_VALIDATION": INVENTORY_INTERNAL_API_VALIDATION,
    "RETRIEVE_INVENTORY_INFO": RETRIEVE_INVENTORY_INFO,
    "RETRIEVE_DEALER_INFO": RETRIEVE_DEALER_INFO,
}
