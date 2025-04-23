class InvalidFilterException(Exception):
    """Exception raised when a filter key is invalid."""
    pass

class ValidationErrorResponse(Exception):
    def __init__(self, errors, full_errors):
        self.errors = errors
        self.full_errors = full_errors

class ErrorMessage(Exception):
    """Exception raised for error messages."""
    def __init__(self, message: str, status_code: int = 400):
        """Initialize the exception with a message and status code."""
        super().__init__(message)
        self.message = message
        self.status_code = status_code