"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

INVENTORY_PARTNER_API_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  basic:
    validateRequestBody: true
    validateRequestParameters: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
CARSALES_CREATE_VEHICLE_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${CarSalesIngestVehicle.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

INVENTORY_PARTNER_API_INTERPOLATION_DATA = {
    "INVENTORY_PARTNER_API_AWS_VALIDATION": INVENTORY_PARTNER_API_AWS_VALIDATION,
    "CARSALES_CREATE_VEHICLE_LAMBDA_INFO": CARSALES_CREATE_VEHICLE_LAMBDA_INFO
}
