import flask
from typing import Optional
from .exceptions import AuthorizationException
from .rawls import *
from .sam import *


def extract_bearer_token(request: flask.Request) -> str:
    token: Optional[str] = request.headers.get("Authorization", type=str)

    if token is None:
        raise AuthorizationException("Missing Authorization: Bearer token in header")

    return token


def workspace_uuid_with_auth(workspace_ns: str, workspace_name: str, bearer_token: str, sam_action: str = "read") -> str:
    ws_uuid = get_workspace_uuid(workspace_ns, workspace_name, bearer_token)

    if sam_action != "read":  # the read check is done when you ask rawls for the workspace UUID, so don't redo it
        if not get_user_action_on_resource("workspace", ws_uuid, sam_action, bearer_token):
            # you can see the workspace, but Sam says you can't do the action to it, so return 403
            raise AuthorizationException()

    return ws_uuid
