

class ISvcException(Exception):
    def __init__(self, message: str, http_status: int = 500):
        self.message = message
        self.http_status = http_status


class BadJsonException(ISvcException):
    def __init__(self, message):
        super().__init__(message, 400)


class AuthorizationException(ISvcException):
    def __init__(self, message: str = "Forbidden"):
        super().__init__(message, 403)


class NotFoundException(ISvcException):
    def __init__(self, message: str = "Not Found"):
        super().__init__(message, 404)


class MethodNotAllowedException(ISvcException):
    def __init__(self, method: str):
        super().__init__(f"Method Not Allowed: {method}", 405)
