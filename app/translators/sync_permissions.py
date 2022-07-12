import logging

from app.db.model import Import, ImportStatus
from app.external import sam

READER_ROLES = ["reader", "writer", "owner", "project-owner"]

def sync_permissions_if_necessary(import_details: Import, import_status: ImportStatus):
    """Check if the status update is for a tdr snapshot sync that just completed, if yes, sync permissions."""
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
    """Get a user's pet token, and use it to sync workspace readers to tdr to give them snapshot read access."""
    # get the proper credentials to call as the user's pet service account
    pet_token = sam.admin_get_pet_auth_header(import_details.workspace_google_project, import_details.submitter)

    # call policy group emails and add them as readers to the snapshot
    for reader_role in READER_ROLES:
        sam.add_child_policy_member("datasnapshot", snapshot_id, sam.READER_POLICY_NAME,
        "workspace", import_details.workspace_uuid, reader_role, pet_token)
    