import logging
import re
import traceback
import uuid
from functools import wraps
from typing import Callable

import flask

from app import db
from app.util.exceptions import *


def httpify_excs(some_func: Callable[..., flask.Response]):
    """Exception handler for "real" (i.e. non-pubsub) endpoints. Turns exceptions into an appropriate HTTP status."""
    @wraps(some_func)
    def catch_excs(*args, **kwargs) -> flask.Response:
        try:
            return some_func(*args, **kwargs)
        except ISvcException as ise:
            # If the exception holds any audit logs, log them
            for alog in ise.audit_logs:
                logging.log(alog.loglevel, alog.msg)
            # Some kind of exception we want to propagate up to the user.
            return flask.make_response(ise.message, ise.http_status)
        except Exception:
            # Anything else is a definite programmer error.
            # Return a 500 and a UUID which developers can look up in the log.
            # NOTE: This will log callstack information and potentially user values.
            eid = uuid.uuid4()
            logging.error(f"eid {eid}:\n{traceback.format_exc()}")
            return flask.make_response(f"Internal Server Error\nerror id: {eid}", 500)

    return catch_excs


def pubsubify_excs(some_func: Callable[..., flask.Response]):
    """Exception handler for pubsub endpoints. Turns exceptions into pubsub responses, and also errors in the db.
    Pubsub interprets the following HTTP status codes as a successful message ack: [102, 200, 201, 202, 204].
    Anything else will make pubsub retry the message; in the majority of cases, this is NOT what we want.
    https://cloud.google.com/pubsub/docs/push#receiving_push_messages
    """
    @wraps(some_func)
    def catch_excs(*args, **kwargs) -> flask.Response:
        try:
            return some_func(*args, **kwargs)
        except ISvcException as ise:
            # If the exception holds any audit logs, log them
            for alog in ise.audit_logs:
                logging.log(alog.loglevel, alog.msg)

            # mark the imports as errored with the associated message.
            with db.session_ctx() as sess:
                for i in ise.imports:
                    newi: Import = Import.reacquire(i.id, sess)
                    newi.write_error(ise.message)

            # Most exceptions just want to mark the import as error'd, but not retry the message delivery.
            return flask.make_response(ise.message, 500 if ise.retry_pubsub else 202)

        except Exception:
            # Anything else is a definite programmer error.
            # Without doing something heinous like intercepting all sqlalchemy queries to keep tab on loaded imports,
            # we can't flip them to errored.
            # It is thus mightily imperative that application code traps exceptions and converts them to
            # IServiceExceptions when they are generated.
            # The best thing we can do is add the error to the log and hope someone notices.
            # NOTE: This will log callstack information and potentially user values.
            eid = uuid.uuid4()
            logging.error(f"eid {eid}:\n{traceback.format_exc()}")
            return flask.make_response(f"Internal Server Error\nerror id: {eid}", 202)  # don't retry mystery errors

    return catch_excs


def _part_to_regex(part: str) -> str:
    r"""Turns <foo> into (?P<foo>[\w\-]+)
    (side note: this docstring has to be a raw string with the r-prefix to prevent a DeprecationWarning)"""
    if len(part) == 0:
        return part
    if part[0] == '<' and part[-1] == '>':
        return r"(?P<" + part[1:-1] + r">[\w-]+)"
    else:
        return part


def _pattern_to_regex(pattern: str) -> str:
    return r'/'.join([_part_to_regex(part) for part in pattern.split('/')])


def expect_urlshape(pattern: str, request_path: str) -> dict:
    """Takes a pattern like "/foo/<boo>/woo" and tests the request path against it.
    Returns {"boo": something} only if the request path matches the pattern and has no slashes in it."""
    regex = _pattern_to_regex(pattern)

    m = re.match(regex, request_path)
    if m is None:
        logging.info(f"couldn't match {request_path} against {pattern}")
        raise NotFoundException()
    else:
        return m.groupdict()
