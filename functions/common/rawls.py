import logging
import os

import requests

from .exceptions import ISvcException


def get_workspace_uuid(workspace_namespace: str, workspace_name: str, bearer_token: str) -> str:
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{workspace_namespace}/{workspace_name}?fields=workspace.workspaceId",
        headers={"Authorization": bearer_token})

    if resp.ok:
        return resp.json()["workspace"]["workspaceId"]
    else:
        # just pass the error upwards
        logging.info(f"rawls.get_workspace_uuid: Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}")
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
        logging.info(f"rawls.check_workspace_iam_action: Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}")
        raise ISvcException(resp.text, resp.status_code)
