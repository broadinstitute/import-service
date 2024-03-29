import logging
import os
from dataclasses import dataclass
from typing import Optional, Set

import requests
from app.external.cloud_platform import CloudPlatform
from app.util.exceptions import ISvcException


@dataclass
class RawlsWorkspaceResponse:
     workspace_id: str
     google_project: str
     cloud_platform: CloudPlatform
     authorization_domain: Optional[Set[str]] = None
     bucket_name: Optional[str] = None


def get_rawls_workspace_info(workspace_namespace: str, workspace_name: str, bearer_token: str) -> RawlsWorkspaceResponse:
    resp = requests.get(
        f"{os.environ.get('RAWLS_URL')}/api/workspaces/{workspace_namespace}/{workspace_name}?fields=workspace.workspaceId,workspace.googleProject,workspace.authorizationDomain,workspace.bucketName,workspace.cloudPlatform",
        headers={"Authorization": bearer_token})

    if resp.ok:
        jso = resp.json()
        return RawlsWorkspaceResponse(
            workspace_id=jso["workspace"]["workspaceId"],
            google_project=jso["workspace"]["googleProject"],
            cloud_platform=jso["workspace"]["cloudPlatform"].lower(),
            authorization_domain=jso["workspace"].get("authorizationDomain"),
            bucket_name=jso["workspace"].get("bucketName")
        )
    else:
        # just pass the error upwards
        workspace_dict = { 'workspace': { 'namespace': workspace_namespace, 'name': workspace_name} }
        logging.info(f"Got {resp.status_code} from Rawls for {workspace_namespace}/{workspace_name}: {resp.text}",
                     extra={"json_fields": workspace_dict})
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
