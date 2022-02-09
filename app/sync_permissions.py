from typing import List

from sqlalchemy.sql.sqltypes import String
from app.db import db
from app.db.model import Import, ImportStatus
from app.external import sam, tdr
from app.external.sam import list_policies_for_resource, WORKSPACE_RESOURCE

READER_ROLES = ["reader", "writer", "owner", "project-owner"]

def sync_permissions_if_necessary(import_job_id: str, import_status: ImportStatus):
    """TODO explain what this does"""
    if import_status != ImportStatus.Done:
        return False # No sync required because import isn't done.

    # get import_job data from db
    with db.session_ctx() as sess:
        import_details = Import.get(import_job_id, sess)

    # if the import job doesn't come with a snapshot id, don't perform a sync
    if import_details.get_snapshot_id() == None:
        # this should mean we aren't doing a tdr-export
        return False # no sync required since no snapshot present
    
    # 4.call syncPermissions(workspace_id, snapshot_id)
    sync_permissions(import_details)

def sync_permissions(import_details: Import) -> bool:
    """TODO explain what this does"""
    # get the proper credentials to call as the user's pet service account
    pet_token = sam.admin_get_pet_token(import_details.workspace_google_project, import_details.submitter)

    # call policy group emails and add them as readers to the snapshot
    policy_group_emails: List[String] = get_policy_group_emails(import_details.workspace_uuid, pet_token)
    for policy_group_email in policy_group_emails: 
        tdr.add_snapshot_policy_member(import_details.get_snapshot_id(), tdr.READER_POLICY_NAME, policy_group_email, pet_token)
    
    return True

def get_policy_group_emails(workspace_id: str, bearer_token: str) -> List[String]:
    """TODO explain what this does""" # does this belong in SAM? or a Facade?
    # TODO get bearer token for user
    policies = list_policies_for_resource(WORKSPACE_RESOURCE, workspace_id, bearer_token)
    # TODO parse policies
    reader_policies = filter(policies, lambda policy: not set(policy.policy.roles).isdisjoint(set(READER_ROLES)))
    return list(map(reader_policies, lambda policy: policy.email))

def add_tdr_reader(policy_email: str):
    """TODO explain what this does""" # does this belong in a TDR Facade?
    # 1. Call TDR to add the group. Get necessary credentials. This function may be unnecessary

"""
[
  {
    "email": "policy-3fe7a136-c743-45ff-bd94-5624e2fb90a9@dev.test.firecloud.org",
    "policy": {
      "actions": [],
      "descendantPermissions": [],
      "memberEmails": [
        "jsafer@broadinstitute.org"
      ],
      "roles": [
        "owner"
      ]
    },
    "policyName": "owner"
  }
]
"""
    