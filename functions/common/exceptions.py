

class ISvcException(Exception):
    def __init__(self, message: str, http_status: int = 500):
        self.message = message
        self.http_status = http_status


class AuthorizationException(ISvcException):
    def __init__(self, message: str = "Forbidden"):
        super().__init__(message, 403)


class NotFoundException(ISvcException):
    def __init__(self, message: str = "Not Found"):
        super().__init__(message, 404)
