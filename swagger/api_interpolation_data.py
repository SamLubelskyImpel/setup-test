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

DMS_DATA_SERVICE_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${DmsDataServiceInbound.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

DMS_DATA_SERVICE_INTERPOLATION_DATA = {
    'DMS_DATA_SERVICE_AWS_VALIDATION': DMS_DATA_SERVICE_AWS_VALIDATION,
    'DMS_DATA_SERVICE_LAMBDA_INFO': DMS_DATA_SERVICE_LAMBDA_INFO
}
