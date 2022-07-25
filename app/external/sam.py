import os

import jsonschema
import logging
import requests
from typing import Any, Dict, List, Optional

from google.auth.transport import requests as grequests
from google.oauth2 import service_account

from app.util.exceptions import AuthorizationException, ISvcException
from app.auth.userinfo import UserInfo
from app.auth import service_auth

DEFAULT_PET_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile"
]
WORKSPACE_RESOURCE = "workspace"

READER_POLICY_NAME = "reader"


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
        logging.debug(f"Got {resp.status_code} from Sam while trying to validate bearer token")
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
        logging.debug(f"Got {resp.status_code} from Sam while checking {action} on resource {resource_type}/{resource_id}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)

def _creds_from_key(key_info: dict, scopes: Optional[List[str]] = None) -> service_account.Credentials:
    """Given a service account key dict from Sam, turn it into a set of Credentials, refreshed with the specified scopes."""
    creds: service_account.Credentials =                                        \
        service_account.Credentials.from_service_account_info(key_info)         \
            .with_scopes(DEFAULT_PET_SCOPES if scopes is None else scopes)
    creds.refresh(grequests.Request())
    return creds


def admin_get_pet_token(google_project: str, user_email: str) -> str:
    """Use our SA to get a token for this user's pet."""
    return _creds_from_key(admin_get_pet_key(google_project, user_email)).token

def admin_get_pet_auth_header(google_project: str, user_email: str) -> str:
    """Use our SA to get a token for this user's pet, formatted as an auth header."""
    return f"Bearer {admin_get_pet_token(google_project, user_email)}"

# Other Terra services have ended up adding a cache here, but given that App Engine VMs spin up and down at will,
# we may not get enough repeated requests on the same machine for an in-memory cache to be worthwhile.
def admin_get_pet_key(google_project: str, user_email: str) -> Dict[str, Any]:
    """Use our SA to get a key for this user's pet."""
    import_svc_token = service_auth.get_isvc_token()
    resp = requests.get(
        f"{os.environ.get('SAM_URL')}/api/google/v1/petServiceAccount/{google_project}/{user_email}",
        headers={"Authorization": f"Bearer {import_svc_token}"})

    if resp.ok:
        return resp.json()
    else:
        logging.debug(f"Got {resp.status_code} from Sam while trying to get pet key for {google_project}/{user_email}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)

def add_child_policy_member(
    parent_resource_type: str, parent_resource_id: str, parent_policy_name: str,
    member_resource_type: str, member_resource_id: str, member_policy_name: str, bearer_token: str) -> None:
    """Add a member to a policy."""
    logging.info(f"SAM request: /api/resources/v2/{parent_resource_type}/{parent_resource_id}/policies/{parent_policy_name}/ \
                    memberPolicies/{member_resource_type}/{member_resource_id}/{member_policy_name}")
    resp = requests.post(
        f"{os.environ.get('SAM_URL')}/api/resources/v2/{parent_resource_type}/{parent_resource_id}/policies/{parent_policy_name}/ \
                    memberPolicies/{member_resource_type}/{member_resource_id}/{member_policy_name}",
        headers={"Authorization": bearer_token}
    )

    if resp.ok:
        logging.info(f"add_child_policy_member succeeded for parent {parent_resource_type}/{parent_resource_id}/{parent_policy_name} \
            and child {member_resource_type}/{member_resource_id}/{member_policy_name}")
        return
    elif resp.status_code == 403:
        logging.error(f"User doesn't have permissions to share policy {parent_resource_type}/{parent_resource_id}/{parent_policy_name} \
            with {member_resource_type}/{member_resource_id}/{member_policy_name}")
        raise AuthorizationException(resp.text)
    else:
        logging.error(f"Error modifying policy for {member_resource_type}/{member_resource_id}/{member_policy_name}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)


def check_health() -> bool:
    resp = requests.get(f"{os.environ.get('SAM_URL')}/status")

    return resp.ok
