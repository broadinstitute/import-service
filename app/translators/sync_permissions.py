from typing import List

from app.db import db
from app.db.model import Import, ImportStatus
from app.external import sam, tdr
from app.external.sam import list_policies_for_resource, WORKSPACE_RESOURCE

READER_ROLES = ["reader", "writer", "owner", "project-owner"]

def sync_permissions_if_necessary(import_job_id: str, import_status: ImportStatus):
    """check if the status update is for a tdr snapshot sync that just completed, if yes, sync permissions"""
    if import_status != ImportStatus.Done:
        return # No sync required because import isn't done.

    # get import_job data from db
    with db.session_ctx() as sess:
        import_details = Import.get(import_job_id, sess)

    # if the import job doesn't come with a snapshot id, don't perform a sync
    snapshot_id = import_details.snapshot_id
    if snapshot_id is None:
        # this should mean we aren't doing a tdr-export
        return # no sync required since no snapshot present
    
    assert snapshot_id is not None
    sync_permissions(import_details, snapshot_id)

def sync_permissions(import_details: Import, snapshot_id: str):
    """get a user's pet token, and use it to sync workspace readers to tdr to give them snapshot read access"""
    # get the proper credentials to call as the user's pet service account
    pet_token = sam.admin_get_pet_token(import_details.workspace_google_project, import_details.submitter)

    # call policy group emails and add them as readers to the snapshot
    policy_group_emails: List[str] = get_policy_group_emails(import_details.workspace_uuid, pet_token)
    for policy_group_email in policy_group_emails: 
        tdr.add_snapshot_policy_member(snapshot_id, tdr.READER_POLICY_NAME, policy_group_email, pet_token)

def get_policy_group_emails(workspace_id: str, bearer_token: str) -> List[str]:
    """call sam to get all policies, and filter out policy group emails for groups that have read access"""
    policies = list_policies_for_resource(WORKSPACE_RESOURCE, workspace_id, bearer_token)

    reader_policies = filter(lambda policy: not set(policy.policy.roles).isdisjoint(set(READER_ROLES)), policies)
    return list(map(lambda policy: policy.email, reader_policies))
    