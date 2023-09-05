import flask.testing
import pytest
from unittest import mock

from app.tests import testutils
from app import new_import
from app.util import exceptions
from app.db import db
from app.db.model import *
from app.external.rawls import RawlsWorkspaceResponse

from app.translate import FILETYPE_TRANSLATORS
from app.new_import import validate_import_url, is_protected_data, is_protected_workspace
from app.util.exceptions import InvalidPathException
from app.auth.userinfo import UserInfo


good_json = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah", "Accept": "application/json"}

good_tdr_json = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "tdrexport", "options": {"tdrSyncPermissions": True}}


@pytest.fixture(scope="function")
def good_http_tdr_manifest(monkeypatch, fake_tdr_manifest):
    monkeypatch.setattr(new_import.http, "http_as_filelike", mock.MagicMock(return_value=fake_tdr_manifest))


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_golden_path(client):
    resp = client.post('/mynamespace/myname/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    assert dbres[0].id == id
    assert resp.headers["Content-Type"] == "application/json"

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env", "good_http_tdr_manifest")
def test_tdr_json_golden_path(client):
    resp = client.post('/mynamespace/myname/imports', json=good_tdr_json, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    assert dbres[0].id == id
    assert dbres[0].is_tdr_sync_required is True # could just assert True, adding check to be explicit
    assert resp.headers["Content-Type"] == "application/json"

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access")
def test_wrong_path(client: flask.testing.FlaskClient):
    resp = client.post('/imports')
    assert resp.status_code == 404


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access")
def test_missing_json(client: flask.testing.FlaskClient):
    resp = client.post('/namespace/name/imports', headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access")
def test_not_json(client):
    resp = client.post('/namespace/name/imports', data="not a json object", headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access")
def test_bad_json(client):
    resp = client.post('/namespace/name/imports', json={"bees":"buzz"}, headers=good_headers)
    assert resp.status_code == 400


def test_bad_token(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers={"Unauthorization?!": "ohno"})
    assert resp.status_code == 403


@pytest.mark.usefixtures(
    testutils.fxpatch(
        "app.external.sam.validate_user",
        side_effect = exceptions.ISvcException("who are you?", 404)))
def test_user_not_found(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    "sam_valid_user",
    testutils.fxpatch(
        "app.auth.user_auth.workspace_uuid_and_project_with_auth",
        side_effect = exceptions.ISvcException("what workspace?", 404)))
def test_user_cant_see_workspace(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    "sam_valid_user",
    testutils.fxpatch(
        "app.auth.user_auth.workspace_uuid_and_project_with_auth",
        side_effect = exceptions.ISvcException("you can't write to this", 403)))
def test_user_cant_write_to_workspace(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 403

# mock function for test_user_cant_read_policies_of_workspace:
# returns ok when asked if the user has "write", but returns 456 when asked if the user has "read_policies"
def mock_auth_for_read_policies(workspace_ns: str, workspace_name: str, bearer_token: str, sam_action: str = "read") -> RawlsWorkspaceResponse:
    if sam_action == "write":
        return RawlsWorkspaceResponse(workspace_id="workspaceId", google_project="googleProject")
    elif sam_action == "read_policies":
        # specify 456 status code to disambiguate the read_policies response from any other response code
        raise exceptions.ISvcException("user does not have read_policies", 456)
    else:
        raise Exception(f"unexpected sam action: ${sam_action}")

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_defaults_true_when_missing_from_json(client):
    json_payload = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert dbres[0].is_upsert

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_is_false_when_false_in_json(client):
    json_payload = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "pfb", "isUpsert": False}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert not dbres[0].is_upsert

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_is_true_when_true_in_json(client):
    json_payload = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "pfb", "isUpsert": True}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert dbres[0].is_upsert

@pytest.mark.parametrize("input_value", ["true", "True", "yes", 1, "false", "False", "no", 0, "", "something else"])
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_bad_request_when_isUpsert_is_not_boolean(input_value, client):
    json_payload = {"path": f"https://{new_import.VALID_NETLOCS[0]}/some/path", "filetype": "pfb", "isUpsert": input_value}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 400

user_info = UserInfo("subject-id", "awesomepossum@broadinstitute.org", True)

@pytest.mark.parametrize("import_url, netloc", [
    ("https://storage.googleapis.com/test-bucket/manifest/files", 'storage.googleapis.com'),
    ("https://test-container.blob.core.windows.net/manifest/files", 'test-container.blob.core.windows.net'),
    ("https://test-bucket.s3.amazonaws.com/manifest/files", 'test-bucket.s3.amazonaws.com'),
    ("storage.googleapis.com", None),
])
@pytest.mark.parametrize("file_type_translator", FILETYPE_TRANSLATORS)
def test_validate_import_url(import_url, netloc, file_type_translator):
    if not netloc:
        with pytest.raises(InvalidPathException):
            validate_import_url(import_url=import_url, import_filetype=file_type_translator, user_info=user_info)
    else:
        assert validate_import_url(import_url=import_url, import_filetype=file_type_translator, user_info=user_info) == netloc

@pytest.mark.parametrize("import_url, protected", [
    ("https://service.prod.anvil.gi.ucsc.edu/path/to/file.pfb", True),
    ("https://service.anvil.gi.ucsc.edu/path/to/file.pfb", True),
    ("https://gen3.biodatacatalyst.nhlbi.nih.gov/path/to/file.pfb", True),
    ("https://something.anvil.edu/path/to/file.pfb", False),
    ("https://something.org/path/to/file.pfb", False),
    ("https://gen3-biodatacatalyst-nhlbi-nih-gov-pfb-export.s3.amazonaws.com/path/to/file.pfb", True),
    ("https://gen3-theanvil-io-pfb-export.s3.amazonaws.com/path/to/file.pfb", True)
])
def test_is_protected_data_pfb(import_url, protected):
    assert is_protected_data(import_url, "pfb", google_project="test_project", user_info=UserInfo("subject-id", "user@example.com", True)) is protected

@pytest.mark.parametrize("manifest,protected", [
    ("test_tdr_response_gcp.json", False),
    ("tdr_manifest_protected_dataset.json", True),
])
def test_is_protected_data_tdr(manifest, protected):
    with mock.patch.object(new_import.http, "http_as_filelike", mock.MagicMock(return_value=open(f"app/tests/resources/{manifest}", "rb"))):
        assert is_protected_data("https://storage.googleapis.com/test-bucket/manifest.json", "tdrexport", google_project="test_project", user_info=UserInfo("subject-id", "user@example.com", True)) is protected

@pytest.mark.parametrize("authorization_domain, bucket_name, protected", [
    (set(), "fc-12345678-a901-23b4-c5d6-7ef8a90b1cd2", False),
    (set(), "fc-secure-12345678-a901-23b4-c5d6-7ef8a90b1cd2", True),
    (set("authDomain"), "fc-secure-12345678-a901-23b4-c5d6-7ef8a90b1cd2", True),
    (set("authDomain"), "fc-12345678-a901-23b4-c5d6-7ef8a90b1cd2", True) #This shouldn't happen in real life
])
def test_is_protected_workspace(authorization_domain, bucket_name, protected):
    assert is_protected_workspace(authorization_domain, bucket_name) is protected
