import logging
import re
import traceback
import uuid
from functools import wraps
from typing import Callable

import flask

from .exceptions import *


def httpify_excs(some_func: Callable[..., flask.Response]):
    """Catches exceptions and turns them into an appropriate HTTP status."""
    @wraps(some_func)
    def catch_excs(*args, **kwargs) -> flask.Response:
        try:
            return some_func(*args, **kwargs)
        except ISvcException as hxc:
            # Some kind of exception we want to propagate up to the user.
            return flask.make_response(hxc.message, hxc.http_status)
        except Exception as e:
            # Anything else is a definite programmer error.
            # Return a 500 and a UUID which developers can look up in the log.
            # NOTE: This will log callstack information and potentially user values.
            eid = uuid.uuid4()
            logging.error(f"eid {eid}:\n{traceback.format_exc()}")
            return flask.make_response(f"Internal Server Error\nerror id: {eid}", 500)

    return catch_excs


def correct_gcf_path(flask_path: str, desired_prefix: str) -> str:
    """In GCF, a cloud function deployed to /foo will strip /foo from the beginning of flask.request.path.
    i.e. GET /foo/bar/baz will give you flask.request.path = "/bar/baz".

    In testing, the test harness puts /foo there.
    i.e. GET /foo/bar/baz will give you flask.request.path = "/foo/bar/baz".

    This function fixes the path in the GCF case to put /foo back."""
    try:
        if flask.current_app.is_test_fixture:  # yuck.
            return flask_path
        else:
            return desired_prefix + flask_path  # won't get here, this never gets explicitly set to false
    except AttributeError:
        return desired_prefix + flask_path  # will get here


def _part_to_regex(part: str) -> str:
    """Turns <foo> into (?P<foo>\w+)"""
    if len(part) == 0:
        return part
    if part[0] == '<' and part[-1] == '>':
        return r"(?P<" + part[1:-1] + r">\w+)"
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
        raise NotFoundException()
    else:
        return m.groupdict()
