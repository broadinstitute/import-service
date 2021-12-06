import logging
import os

import requests

from app.util.exceptions import ISvcException
from dataclasses import dataclass
from typing import Dict


@dataclass
class RawlsWorkspaceResponse:
     workspace_id: str
     google_project: str


def get_workspace_uuid_and_project(workspace_namespace: str, workspace_name: str, bearer_token: str) -> RawlsWorkspaceResponse:
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{workspace_namespace}/{workspace_name}?fields=workspace.workspaceId,workspace.googleProject",
        headers={"Authorization": bearer_token})

    if resp.ok:
        jso = resp.json()
        return RawlsWorkspaceResponse(workspace_id=jso["workspace"]["workspaceId"], google_project=jso["workspace"]["googleProject"])
    else:
        # just pass the error upwards
        logging.info(f"Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)


def check_workspace_iam_action(workspace_namespace: str, workspace_name: str, action: str, bearer_token: str) -> bool:
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{workspace_namespace}/{workspace_name}/checkIamActionWithLock/{action}",
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
