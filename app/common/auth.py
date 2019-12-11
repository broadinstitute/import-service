from typing import Optional

import flask
import logging

from .exceptions import AuthorizationException
from ..common import rawls
from ..common import sam


def extract_auth_token(request: flask.Request) -> str:
    """Given an incoming Flask request, extract the value of the Authorization header"""
    token: Optional[str] = request.headers.get("Authorization", type=str)

    if token is None:
        logging.info("Missing Authorization header")
        raise AuthorizationException("Missing Authorization: Bearer token in header")

    return token


def workspace_uuid_with_auth(workspace_ns: str, workspace_name: str, bearer_token: str, sam_action: str = "read") -> str:
    """Checks Rawls to get the workspace UUID, and then checks Sam to see if the user has the given action on the workspace resource.
    If so, returns the workspace UUID."""
    ws_uuid = rawls.get_workspace_uuid(workspace_ns, workspace_name, bearer_token)

    # the read check is done when you ask rawls for the workspace UUID, so don't redo it
    if sam_action != "read" and not rawls.check_workspace_iam_action(workspace_ns, workspace_name, sam_action, bearer_token):
        # you can see the workspace, but you can't do the action to it (potentially also because the workspace is locked), so return 403
        logging.info(f"User has read action on workspace {workspace_ns}/{workspace_name}, but cannot perform {sam_action}.")
        raise AuthorizationException(f"Cannot perform the action {sam_action} on {workspace_ns}/{workspace_name}.")

    return ws_uuid
