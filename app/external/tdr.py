import logging
import requests
import os

from app.util.exceptions import AuthorizationException, ISvcException

READER_POLICY_NAME = 'reader'

def add_snapshot_policy_member(snapshot_id: str, policy_name: str, member_email: str, bearer_token: str) -> None:
    """Add a member to a snapshot policy"""
    resp = requests.post(
        f"{os.environ.get('TDR_URL')}/api/repository/v1/snapshots/{snapshot_id}/policies/{policy_name}/members",
        headers={"Authorization": bearer_token},
        json={"email": member_email}
    )

    if resp.ok:
        return
    elif resp.status_code == 403:
        logging.error(f"User doesn't have permissions to share snapshot {snapshot_id} policy {policy_name} with {member_email}")
        raise AuthorizationException(resp.text)
    else:
        logging.error(f"Error syncing snapshot permissions for snapshot {snapshot_id} policy {policy_name}", resp)
        raise ISvcException(resp.text, resp.status_code)
