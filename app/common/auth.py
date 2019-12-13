from typing import Optional

import flask
import logging
import os
import traceback

from google.auth.transport import requests as grequests  # type: ignore
from google.oauth2 import id_token  # type: ignore

from .exceptions import AuthorizationException, BadPubSubTokenException
from ..common import rawls


def verify_pubsub_jwt(request: flask.Request) -> None:
    """Verify that this request came from Cloud Pub/Sub.
    This looks for a secret token in a queryparam, then decodes the Bearer token
    and checks identity and audience.

    See here: https://cloud.google.com/pubsub/docs/push#using_json_web_tokens_jwts"""
    if request.args.get('token', '') != os.environ.get('PUBSUB_TOKEN'):
        logging.info("Bad Pub/Sub token")
        raise BadPubSubTokenException()

    bearer_token = request.headers.get('Authorization', '')
    token = bearer_token.split(' ', maxsplit=1)[1]

    try:
        claim = id_token.verify_oauth2_token(token, grequests.Request(),
                                             audience=os.environ.get('PUBSUB_AUDIENCE'))
        if claim['iss'] not in [
            'accounts.google.com',
            'https://accounts.google.com'
        ]:
            # bad issuer
            logging.info("Bad issuer")
            raise BadPubSubTokenException()

        if claim['email'] != os.environ.get('PUBSUB_ACCOUNT'):
            logging.info("Incorrect email address")
            raise BadPubSubTokenException()
    except Exception as e:
        # eats all exceptions, including ones thrown by verify_oauth2_token if e.g. audience is wrong
        logging.info(traceback.format_exc())
        raise BadPubSubTokenException()


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
