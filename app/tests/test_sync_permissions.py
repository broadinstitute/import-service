from copy import deepcopy
from unittest import mock
from app.db import model
from app.translators.sync_permissions import sync_permissions, sync_permissions_if_necessary


def test_sync_permissions_for_tdr_snapshot(fake_import_tdr_manifest_gcp_gs: model.Import):
    finished_import = deepcopy(fake_import_tdr_manifest_gcp_gs)
    finished_import.snapshot_id = "12_34"
    finished_import.is_tdr_sync_required = True
    with mock.patch("app.translators.sync_permissions.sync_permissions") as mock_sync:
        sync_permissions_if_necessary(finished_import, model.ImportStatus.Done)
        mock_sync.assert_called_once()

def test_no_sync_for_tdr_snapshot_if_not_required(fake_import_tdr_manifest_gcp_gs: model.Import):
    finished_import = deepcopy(fake_import_tdr_manifest_gcp_gs)
    finished_import.snapshot_id = "12_34"
    finished_import.is_tdr_sync_required = False
    with mock.patch("app.translators.sync_permissions.sync_permissions") as mock_sync:
        sync_permissions_if_necessary(finished_import, model.ImportStatus.Done)
        mock_sync.assert_not_called()

def test_no_sync_for_pfb(fake_import: model.Import):
    finished_import = deepcopy(fake_import)
    with mock.patch("app.translators.sync_permissions.sync_permissions") as mock_sync:
        sync_permissions_if_necessary(finished_import, model.ImportStatus.Done)
        mock_sync.assert_not_called()

def test_no_sync_if_snapshot_import_not_completed(fake_import_tdr_manifest_gcp_gs: model.Import):
    finished_import = deepcopy(fake_import_tdr_manifest_gcp_gs)
    finished_import.snapshot_id = "12_34"
    with mock.patch("app.translators.sync_permissions.sync_permissions") as mock_sync:
        sync_permissions_if_necessary(finished_import, model.ImportStatus.Upserting)
        mock_sync.assert_not_called()

def test_all_readers_are_synced(fake_import_tdr_manifest_gcp_gs: model.Import):
    with mock.patch("app.external.sam.admin_get_pet_auth_header") as mock_token:
        mock_token.return_value = "fake_token"
        with mock.patch("app.external.sam.add_child_policy_member") as mock_update_policy:
            sync_permissions(fake_import_tdr_manifest_gcp_gs, "12_34")

            mock_token.assert_called_once_with(fake_import_tdr_manifest_gcp_gs.workspace_google_project, fake_import_tdr_manifest_gcp_gs.submitter)
            mock_update_policy.assert_called_with("datasnapshot", "12_34", "reader", "workspace", "uuid2", "project-owner", "fake_token")
            assert mock_update_policy.call_count == 4
