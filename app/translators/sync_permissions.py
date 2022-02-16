import logging
from typing import List

from app.db.model import Import, ImportStatus
from app.external import sam, tdr

READER_ROLES = ["reader", "writer", "owner", "project-owner"]

def sync_permissions_if_necessary(import_details: Import, import_status: ImportStatus):
    """check if the status update is for a tdr snapshot sync that just completed, if yes, sync permissions."""
    if import_status != ImportStatus.Done:
        return # No sync required because import isn't done.

    # if the import job doesn't come with a snapshot id, don't perform a sync
    snapshot_id = import_details.snapshot_id
    if snapshot_id is None:
        # this should mean we aren't doing a tdr-export
        if import_details.filetype == "tdrexport":
            logging.error(f"Import {import_details.id} has filetype tdrexport, but no snapshot id is recorded for permission syncing.")
        return # no sync required since no snapshot present

    assert snapshot_id is not None

    logging.info(f"Syncing permissions for import {import_details.id} for snapshot {snapshot_id}")
    sync_permissions(import_details, snapshot_id)

def sync_permissions(import_details: Import, snapshot_id: str):
    """get a user's pet token, and use it to sync workspace readers to tdr to give them snapshot read access."""
    # get the proper credentials to call as the user's pet service account
    pet_token = sam.admin_get_pet_token(import_details.workspace_google_project, import_details.submitter)
    # TODO next line for dev/debug only, remove-
    logging.info("pet token {pet_token}")

    # call policy group emails and add them as readers to the snapshot
    policy_group_emails: List[str] = get_policy_group_emails(import_details.workspace_uuid, pet_token)
    for policy_group_email in policy_group_emails:
        tdr.add_snapshot_policy_member(snapshot_id, tdr.READER_POLICY_NAME, policy_group_email, pet_token)

def get_policy_group_emails(workspace_id: str, bearer_token: str) -> List[str]:
    """call sam to get all policies, and filter out policy group emails for groups that have read access."""
    policies = sam.list_policies_for_resource(sam.WORKSPACE_RESOURCE, workspace_id, bearer_token)

    reader_policies = filter(lambda policy: len(set(policy.policy.roles).intersection(set(READER_ROLES))) > 0, policies)
    return list(map(lambda policy: policy.email, reader_policies))
    