import logging
import os

import requests
from requests import Request, Session

from app.util.exceptions import ISvcException
import urllib

def encode(param: str) -> str:
    return urllib.parse.quote(param)

def get_workspace_uuid(workspace_namespace: str, workspace_name: str, bearer_token: str) -> str:
    s = Session()
    encoded_workspace_namespace = encode(workspace_namespace)
    encoded_workspace_name = encode(workspace_name)
    url = f"{os.environ.get('RAWLS_URL')}/api/workspaces/{encoded_workspace_namespace}/{encoded_workspace_name}?fields=workspace.workspaceId"
    req = Request('GET', url, headers={"Authorization": bearer_token})
    prepped = s.prepare_request(req)
    prepped.url = url
    resp = s.send(prepped)

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
