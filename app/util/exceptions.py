from typing import List, Optional
from app.auth.userinfo import UserInfo

class ISvcException(Exception):
    def __init__(self, message: str, http_status: int = 500, audit_logs: List[str] = []):
        self.message = message
        self.http_status = http_status
        self.audit_logs = audit_logs

class BadJsonException(ISvcException):
    def __init__(self, message):
        super().__init__(message, 400)


class BadPubSubTokenException(ISvcException):
    def __init__(self):
        # This is a deliberately unhelpful message so we don't give details to attackers.
        super().__init__("Invalid request", 400)


class AuthorizationException(ISvcException):
    def __init__(self, message: str = "Forbidden"):
        super().__init__(message, 403)


class NotFoundException(ISvcException):
    def __init__(self, message: str = "Not Found"):
        super().__init__(message, 404)


class MethodNotAllowedException(ISvcException):
    def __init__(self, method: str):
        super().__init__(f"Method Not Allowed: {method}", 405)

class InvalidPathException(ISvcException):
    def __init__(self, import_url: Optional[str], user_info: UserInfo, hint: str):
        audit_logs = [f"User {user_info.subject_id} {user_info.user_email} attempted to import from path {import_url}"]
        super().__init__(f"Path Not Allowed - {hint}: {import_url}", 400, audit_logs)
