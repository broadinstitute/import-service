import pytest
import unittest.mock as mock

from app import translate
from app.db import db
from app.db.model import Import, ImportStatus
from app.server.requestutils import PUBSUB_STATUS_NOTOK
from app.tests import testutils

good_json = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah"}


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_get_import_status(client):
    new_import_resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert new_import_resp.status_code == 201
    import_id = new_import_resp.json["jobId"]

    resp = client.get(f'/namespace/name/imports/{import_id}', headers=good_headers)
    assert resp.status_code == 200
    assert resp.json == {'jobId': import_id, 'filetype': 'pfb', 'status': ImportStatus.Pending.name}


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_get_import_status_404(client):
    fake_id = "fake_id"
    resp = client.get('/namespace/name/imports/{}'.format(fake_id), headers=good_headers)
    assert resp.status_code == 404
    assert fake_id in resp.get_data(as_text=True)


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_get_all_import_status(fake_import, client):
    with db.session_ctx() as sess:
        fake_import.status = ImportStatus.Error
        fake_import.error_message = "broke"
        sess.add(fake_import)

    resp = client.get('/aa/aa/imports', headers=good_headers)
    assert resp.status_code == 200
    assert resp.json == [{"jobId": fake_import.id, "filetype": "pfb", "status": ImportStatus.Error.name, 'message': "broke"}]


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_get_all_running_when_none(client):
    # poke in one import that's in the Done state
    with db.session_ctx() as sess:
        new_import = Import("namespace", "name", "uuid", "project", "hello@me.com", "http://path", "pfb")
        new_import.status = ImportStatus.Done
        sess.add(new_import)
        sess.commit()
        dbres = sess.query(Import).all()
        assert len(dbres) == 1

    resp = client.get('/namespace/name/imports?running_only', headers=good_headers)
    assert resp.status_code == 200
    assert resp.json == []


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_get_all_running_with_one(client):
    import_id = client.post('/namespace/name/imports', json=good_json, headers=good_headers).json["jobId"]

    resp = client.get('/namespace/name/imports?running_only', headers=good_headers)
    assert resp.status_code == 200
    assert resp.json == [{"jobId": import_id, "filetype": "pfb", "status": ImportStatus.Pending.name}]


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_good_update_status(fake_import, client):
    """External service moves import from existing status to wherever."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "current_status": "Pending",
                                                        "new_status": "Upserting"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Upserting

    assert resp.status_code == 200

@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_tdr_upsert_completed_status(fake_import, client):
    """External service moves import from existing status to wherever."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    with db.session_ctx() as sess2:
        Import.save_snapshot_id_exclusively(fake_import.id, "fake_snapshot_id", sess2)

    # sam return a list of policies
    list_of_policies = [{
        "email": "testtest@broad.io",
        "policyName": "readerThing",
        "policy": {
            "roles": ["owner"],
            "memberEmails": ["test@broad.io"],
            "actions": ["read"]
    }}]

    # monkey patch sam and tdr
    with testutils.patch_request("app.external.sam", "get", 200, json=list_of_policies):
        with testutils.patch_request("app.external.tdr", "post", 200):
            with mock.patch("app.external.sam.admin_get_pet_token") as mock_token:
                mock_token.return_value = "fake_token"
                resp = client.post("/_ah/push-handlers/receive_messages",
                                json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                                    "current_status": "Upserting",
                                                                    "new_status": "Done"}))

    with db.session_ctx() as sess3:
        imp: Import = Import.get(fake_import.id, sess3)
        assert imp.status == ImportStatus.Done

    assert resp.status_code == 200


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_good_update_status_wrong_current(fake_import, client):
    """External service attempts to move import from wrong current status to wherever."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    # as long as we're moving forward, we ignore current_status
    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "current_status": "ReadyForUpsert",
                                                        "new_status": "Upserting"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Upserting

    assert resp.status_code == 200


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_good_update_status_to_error_with_message(fake_import, client):
    """External service moves import from wherever to Error, providing a message."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "new_status": "Error",
                                                        "error_message": "blah"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Error
        assert "blah" in imp.error_message

    assert resp.status_code == 200


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_good_update_status_to_error_no_message(fake_import, client):
    """External service moves import from wherever to Error, providing no message."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "new_status": "Error"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Error
        assert imp.error_message == "External service set this import to Error"

    assert resp.status_code == 200


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_fail_update_status_from_terminal(fake_import: Import, client):
    """External service attempts to move import from terminal status, fails."""
    with db.session_ctx() as sess:
        fake_import.status = ImportStatus.Done
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "current_status": "Pending",
                                                        "new_status": "Upserting"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Done  # unchanged

    assert resp.status_code == PUBSUB_STATUS_NOTOK

@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_fail_update_status_backwards(fake_import: Import, client):
    """External service attempts to move import backwards in status, fails."""
    with db.session_ctx() as sess:
        fake_import.status = ImportStatus.Upserting
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "status", "import_id": fake_import.id,
                                                        "current_status": "Upserting",
                                                        "new_status": "Pending"}))

    with db.session_ctx() as sess2:
        imp: Import = Import.get(fake_import.id, sess2)
        assert imp.status == ImportStatus.Upserting  # unchanged

    assert resp.status_code == PUBSUB_STATUS_NOTOK
