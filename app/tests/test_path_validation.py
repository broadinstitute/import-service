import flask.testing
import pytest
import os
from unittest.mock import patch, MagicMock

from app.auth.userinfo import UserInfo

from app import translate, new_import

good_headers = {"Authorization": "Bearer ya29.blahblah"}

os.environ.setdefault("BATCH_UPSERT_BUCKET", "unittest-allowed-bucket")

def assert_response_code_and_logs(resp, caplog, import_url):
    assert resp.status_code == 400
    # if the "sam_valid_user" fixture changes, this assertion will also need to change
    auditlog = filter(lambda rec: rec.message == f"User 123456 hello@bees.com attempted to import from path {import_url}", caplog.records)
    assert list(auditlog), "Expected audit log message to exist if user specified illegal domain; did not find such message in log."


@pytest.fixture(scope="function")
def good_http_tdr_manifest(monkeypatch, fake_tdr_manifest):
    monkeypatch.setattr(new_import.http, "http_as_filelike", MagicMock(return_value=fake_tdr_manifest))


user_info = UserInfo("subject-id", "awesomepossum@broadinstitute.org", True)
@pytest.mark.parametrize("netloc", ["storage.googleapis.com",
                                    "test-container.blob.core.windows.net",
                                    "test-bucket.s3.amazonaws.com",
                                    "s3.amazonaws.com"])
@pytest.mark.parametrize("filetype", translate.FILETYPE_TRANSLATORS.keys())
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env", "good_http_tdr_manifest")
def test_default_valid_netlocs(client, netloc, filetype):
    path = f"https://{netloc}/some/valid/path"
    payload = {"path": path, "filetype": filetype}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert resp.status_code == 201
    # NB we don't test anything deeper than the 201 response; other tests check to see if the
    # db is updated, etc.

@patch.object(new_import, "VALID_NETLOCS", ["exact-test.example.com", "*subdomain-test.example.com"])
@pytest.mark.parametrize("netloc", ["exact-test.example.com",
                                    "subdomain-test.example.com",
                                    "subdomain.subdomain-test.example.com"])
@pytest.mark.parametrize("filetype", translate.FILETYPE_TRANSLATORS.keys())
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env", "good_http_tdr_manifest")
def test_configured_valid_netlocs(client, netloc, filetype):
    path = f"https://{netloc}/some/valid/path"
    payload = {"path": path, "filetype": filetype}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert resp.status_code == 201
    # NB we don't test anything deeper than the 201 response; other tests check to see if the
    # db is updated, etc.

@patch.object(new_import, "VALID_NETLOCS", ["example.com"])
@pytest.mark.parametrize("path", ["https://haxxor.evil.bad/some/valid/path", "https://test.example.com/path/to/file"])
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
@pytest.mark.parametrize("filetype", translate.FILETYPE_TRANSLATORS.keys())
def test_invalid_netlocs(client: flask.testing.FlaskClient, filetype, caplog, path):
    payload = {"path": path, "filetype": filetype}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_unparsable_path(client: flask.testing.FlaskClient, caplog):
    payload = {"path": f"https://[:-999/~~~~~~~~~~~", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
@pytest.mark.parametrize("path,filetype", [
    ("gs://bucket https://example.com/foo", "tdrexport"),
    ("https://*/foo", "tdrexport"),
    ("https://example.com https://example.com", "pfb"),
])
def test_invalid_url(client, path, filetype):
    payload = {"path": path, "filetype": filetype}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", new_import.VALID_NETLOCS)
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_netloc_as_subdomain_of_bad_tld(client, netloc, caplog):
    payload = {"path": f"https://{netloc}.evil/some/valid/path", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.parametrize("netloc", new_import.VALID_NETLOCS)
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_netloc_in_fragment(client, netloc, caplog):
    payload = {"path": f"https://evil.bad/some/valid/path#{netloc}", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.parametrize("netloc", new_import.VALID_NETLOCS)
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_netloc_in_query(client, netloc, caplog):
    payload = {"path": f"https://evil.bad/some/valid/path?q={netloc}", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.parametrize("netloc", new_import.VALID_NETLOCS)
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_netloc_in_path(client, netloc, caplog):
    payload = {"path": f"https://evil.bad/hide/{netloc}/in/path", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_audit_logging(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "https://illegal.domains/should/be/logged", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_bucket_for_rawlsjson(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "gs://unittest-allowed-bucket/some/valid/path.json", "filetype": "rawlsjson"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert resp.status_code == 201

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_bucket_for_rawlsjson_in_path(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "gs://unittest-DISallowed-bucket/some/unittest-allowed-bucket/path.json", "filetype": "rawlsjson"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_illegal_bucket_for_rawlsjson(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "gs://unittest-DISallowed-bucket/some/valid/path.json", "filetype": "rawlsjson"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.parametrize("netloc", new_import.VALID_NETLOCS)
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_netlocs_but_rawlsjson(client, netloc, caplog):
    payload = {"path": f"https://{netloc}/some/valid/path", "filetype": "rawlsjson"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_legal_bucket_but_pfb(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "gs://unittest-allowed-bucket/some/valid/path.json", "filetype": "pfb"}
    resp = client.post('/namespace/name/imports', json=payload, headers=good_headers)
    assert_response_code_and_logs(resp, caplog, payload["path"])
