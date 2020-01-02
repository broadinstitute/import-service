import logging
import re
import traceback
import uuid
from functools import wraps
from typing import Callable

import flask

from app.util.exceptions import *


def httpify_excs(some_func: Callable[..., flask.Response]):
    """Catches exceptions and turns them into an appropriate HTTP status."""
    @wraps(some_func)
    def catch_excs(*args, **kwargs) -> flask.Response:
        try:
            return some_func(*args, **kwargs)
        except ISvcException as hxc:
            # Some kind of exception we want to propagate up to the user.
            return flask.make_response(hxc.message, hxc.http_status)
        except Exception:
            # Anything else is a definite programmer error.
            # Return a 500 and a UUID which developers can look up in the log.
            # NOTE: This will log callstack information and potentially user values.
            eid = uuid.uuid4()
            logging.error(f"eid {eid}:\n{traceback.format_exc()}")
            return flask.make_response(f"Internal Server Error\nerror id: {eid}", 500)

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
