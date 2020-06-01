import logging
import os

import requests
import urllib

from app.util.exceptions import ISvcException

def encode(param):
    urllib.parse.urlencode(param, quote_via=urllib.parse.quote)

def get_workspace_uuid(workspace_namespace: str, workspace_name: str, bearer_token: str) -> str:
    encoded_workspace_namespace = encode(workspace_namespace)
    encoded_workspace_name = encode(workspace_name)
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{encoded_workspace_namespace}/{encoded_workspace_name}?fields=workspace.workspaceId",
        headers={"Authorization": bearer_token})

    if resp.ok:
        return resp.json()["workspace"]["workspaceId"]
    else:
        # just pass the error upwards
        logging.info(f"Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)


def check_workspace_iam_action(workspace_namespace: str, workspace_name: str, action: str, bearer_token: str) -> bool:
    encoded_workspace_namespace = encode(workspace_namespace)
    encoded_workspace_name = encode(workspace_name)
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{encoded_workspace_namespace}/{encoded_workspace_name}/checkIamActionWithLock/{action}",
        headers={"Authorization": bearer_token})

    if resp.ok:
        return True
    elif resp.status_code == 403:
        return False
    else:
        # just pass the error upwards
        logging.info(f"Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)


def check_health() -> bool:
    resp = requests.get(f"{os.environ.get('RAWLS_URL')}/status")

    return resp.ok
