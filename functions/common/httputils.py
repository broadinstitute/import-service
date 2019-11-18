import flask
from functools import wraps
import logging
import traceback
from typing import Callable
import uuid
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
