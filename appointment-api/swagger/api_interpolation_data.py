"""Contains interpolation data for the swagger api template.
This is data that we do not want to use for the client copy, but need for deployment.
As such this data is interpolated with the client copy as part of deployment script."""

APPOINTMENT_SERVICE_API_VALIDATION = """
x-amazon-apigateway-request-validators:
  full:
    validateRequestBody: true
    validateRequestParameters: true
    validateRequestHeaders: true

x-amazon-apigateway-request-validator: full
"""

# httpMethod should always be POST even for other endpoint types
CREATE_APPOINTMENT_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${CreateAppointment.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_APPOINTMENTS_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveAppointments.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

RETRIEVE_TIMESLOTS_INFO = """
x-amazon-apigateway-integration:
  uri:
    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${RetrieveTimeslots.Arn}/invocations
  passthroughBehavior: never
  httpMethod: POST
  type: aws_proxy
"""

APPOINTMENT_SERVICE_API_INTERPOLATION_DATA = {
    "APPOINTMENT_SERVICE_API_VALIDATION": APPOINTMENT_SERVICE_API_VALIDATION,
    "CREATE_APPOINTMENT_INFO": CREATE_APPOINTMENT_INFO,
    "RETRIEVE_APPOINTMENTS_INFO": RETRIEVE_APPOINTMENTS_INFO,
    "RETRIEVE_TIMESLOTS_INFO": RETRIEVE_TIMESLOTS_INFO
}
