"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

VDP_API_AWS_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
QUERY_VDP_LAMBDA_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${QueryVDP.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

VDP_API_INTERPOLATION_DATA = {
    "VDP_API_AWS_VALIDATION": VDP_API_AWS_VALIDATION,
    "GET_VDP_LAMBDA_INFO": QUERY_VDP_LAMBDA_INFO,
}
