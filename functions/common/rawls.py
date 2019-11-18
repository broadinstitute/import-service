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
        raise ISvcException(resp.text, resp.status_code)
