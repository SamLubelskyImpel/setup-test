"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

DMS_DATA_SERVICE_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
REPAIR_ORDER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RepairOrderInbound.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

VEHICLE_SALE_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${VehicleSaleInbound.Arn}/invocations
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

INTEGRATION_PARTNER_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${IntegrationPartner.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

DMS_DATA_SERVICE_INTERPOLATION_DATA = {
    'DMS_DATA_SERVICE_AWS_VALIDATION': DMS_DATA_SERVICE_AWS_VALIDATION,
    'REPAIR_ORDER_LAMBDA_INFO': REPAIR_ORDER_LAMBDA_INFO,
    'VEHICLE_SALE_LAMBDA_INFO': VEHICLE_SALE_LAMBDA_INFO,
    'DEALER_LAMBDA_INFO': DEALER_LAMBDA_INFO,
    'INTEGRATION_PARTNER_LAMBDA_INFO': INTEGRATION_PARTNER_LAMBDA_INFO
}
