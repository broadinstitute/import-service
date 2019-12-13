import os

import jsonschema
import logging
import requests

from .exceptions import AuthorizationException, ISvcException
from .userinfo import UserInfo


def validate_user(bearer_token: str) -> UserInfo:
    schema = {
        "type": "object",
        "required": ["userSubjectId", "userEmail", "enabled"],
        "properties": {
            "userSubjectId": {"type": "string"},
            "userEmail": {"type": "string"},
            "enabled": {"type": "boolean"}
        }
    }

    resp = requests.get(
        f"{os.environ.get('SAM_URL')}/register/user/v2/self/info",
        headers={"Authorization": bearer_token})

    if resp.ok:
        uinfo = resp.json()
        jsonschema.validate(uinfo, schema=schema)
        user_info = UserInfo(uinfo["userSubjectId"], uinfo["userEmail"], uinfo["enabled"])
        if not user_info.enabled:
            logging.info(f"User {uinfo['userSubjectId']} is not enabled")
            raise AuthorizationException("Not enabled")
        return user_info
    elif resp.status_code == 404:
        logging.info(f"Not registered at all in Terra")
        raise AuthorizationException("Not registered")
    else:
        logging.info(f"Got {resp.status_code} from Sam while trying to validate bearer token")
        raise ISvcException(resp.text, resp.status_code)


def get_user_action_on_resource(resource_type: str, resource_id: str, action: str, bearer_token: str) -> bool:
    """Returns if the user has access on the given resource."""
    resp = requests.get(
        f"{os.environ.get('SAM_URL')}/api/resources/v1/{resource_type}/{resource_id}/action/{action}",
        headers={"Authorization": bearer_token})

    if resp.ok:
        jsonschema.validate(resp.json(), schema={"type": "boolean"})  # will raise an exc if body is wrong, to be caught upstream
        return resp.json()
    else:
        logging.info(f"Got {resp.status_code} from Sam while checking {action} on resource {resource_type}/{resource_id}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)