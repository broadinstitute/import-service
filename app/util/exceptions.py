import uuid
import logging
import traceback

from typing import Optional, List, NamedTuple
from app.db.model import Import, ImportStatus
from app.auth.userinfo import UserInfo


class AuditLog(NamedTuple):
    msg: str
    loglevel: int  # one of the log levels [CRITICAL, ERROR, WARNING, INFO, DEBUG], not just your favourite int


class ISvcException(Exception):
    def __init__(self, message: str, http_status: int = 500, imports: Optional[List[Import]] = None, audit_logs: Optional[List[AuditLog]] = None, retry_pubsub: bool = False):
        self.message = message
        self.http_status = http_status
        self.retry_pubsub = retry_pubsub
        self.audit_logs = audit_logs if audit_logs else []
        self.imports = imports if imports else []


class BadJsonException(ISvcException):
    def __init__(self, message, audit_log: bool = True):
        audit_logs = [AuditLog(message, logging.WARN)] if audit_log else None
        super().__init__(message, 400, audit_logs=audit_logs)


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
        audit_logs = [AuditLog(f"User {user_info.subject_id} {user_info.user_email} attempted to import from path {import_url}", logging.ERROR)]
        super().__init__(f"Path Not Allowed - {hint}: {import_url}", 400, audit_logs=audit_logs)

class InvalidFiletypeException(ISvcException):
    def __init__(self, import_filetype: Optional[str], user_info: UserInfo, hint: str):
        audit_logs = [AuditLog(f"User {user_info.subject_id} {user_info.user_email} attempted to import from filetype {import_filetype}", logging.ERROR)]
        super().__init__(f"Path Not Allowed - {hint}: {import_filetype}", 400, audit_logs=audit_logs)

class FileTranslationException(ISvcException):
    def __init__(self, imprt: Import, exc: Exception):
        eid = uuid.uuid4()
        tb = exc.__traceback__

        user_msg = f"Error translating file (eid: {str(eid)}). This file is likely corrupt or contains illegal syntax. " + \
                   f"Please check your file for validity before trying again. Underlying error message: " + \
                   f"{str(exc)} for file {imprt.import_url}"

        audit_logs = [AuditLog(f"Error translating import id: {imprt.id} \n" +
                               f"file: {imprt.import_url} \n" +
                               f"eid {eid}: \n" +
                               f"{''.join(traceback.format_tb(tb))}", logging.WARN)]
        super().__init__(user_msg, 500, [imprt], audit_logs=audit_logs)


class SystemException(ISvcException):
    """For programmer-related, "oh no this is definitely a bug" / "we configured something badly" type errors."""
    def __init__(self, imprts: Optional[List[Import]], exc: Exception):
        eid = uuid.uuid4()
        tb = exc.__traceback__

        user_msg = f"System error. Please file a bug report.\n" + \
                   f"{str(exc)}\n" + \
                   f"eid: {str(eid)}"

        audit_logs = [AuditLog(f"System error:\n" +
                               f"eid {eid}: \n" +
                               f"{''.join(traceback.format_tb(tb))}", logging.ERROR)]
        super().__init__(user_msg, 500, imprts, audit_logs=audit_logs)


class TerminalStatusChangeException(ISvcException):
    """An external service requested changing the state of an import, but the import was already in a terminal state."""
    def __init__(self, import_id: str, requested_status: ImportStatus, current_terminal_status: ImportStatus):
        eid = uuid.uuid4()
        msg = f"Requested illegal status change on import {import_id} from terminal status {current_terminal_status} to {requested_status}"
        audit_logs = [AuditLog(msg, logging.WARN)]
        super().__init__(msg, 400, audit_logs=audit_logs)

class IllegalStatusChangeException(ISvcException):
    """An external service requested changing the state of an import, but the new state is not legal compared to the current state."""
    def __init__(self, import_id: str, requested_status: ImportStatus, current_terminal_status: ImportStatus):
        eid = uuid.uuid4()
        msg = f"Requested illegal status change on import {import_id} from status {current_terminal_status} to {requested_status}"
        audit_logs = [AuditLog(msg, logging.WARN)]
        super().__init__(msg, 400, audit_logs=audit_logs)
