import flask.testing
import jsonschema
import pytest

from app.tests import testutils
from app import service, translate
from app.auth import userinfo
from app.db.model import *


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


good_json = {"path": "foo", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah"}

sam_valid_user = testutils.fxpatch(
    "app.external.sam.validate_user",
    return_value=userinfo.UserInfo("123456", "hello@bees.com", True))

user_has_ws_access = testutils.fxpatch(
    "app.auth.user_auth.workspace_uuid_with_auth",
    return_value="some-uuid")

# replace the publish to google pub/sub with a no-op one
pubsub_publish = testutils.fxpatch(
    "app.external.pubsub.publish")


@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_legal_netlocs_simple(client, netloc):
    payload = {"path": f"https://{netloc}/some/valid/path", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 200
    # NB we don't test anything deeper than the 200 response; other tests check to see if the
    # db is updated, etc.

@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_illegal_netloc_simple(client: flask.testing.FlaskClient):
    payload = {"path": f"https://haxxor.evil.bad/some/valid/path", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_subdomain_of_legal_netlocs(client, netloc):
    payload = {"path": f"https://hijacked.{netloc}/some/valid/path", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_legal_netloc_as_subdomain_of_bad_tld(client, netloc):
    payload = {"path": f"https://{netloc}.evil/some/valid/path", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_legal_netloc_in_fragment(client, netloc):
    payload = {"path": f"https://evil.bad/some/valid/path#{netloc}", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_legal_netloc_in_query(client, netloc):
    payload = {"path": f"https://evil.bad/some/valid/path?q={netloc}", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.parametrize("netloc", translate.VALID_NETLOCS)
@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_legal_netloc_in_path(client, netloc):
    payload = {"path": f"https://evil.bad/hide/{netloc}/in/path", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_audit_logging(client: flask.testing.FlaskClient, caplog):
    payload = {"path": "https://illegal.domains/should/be/logged", "filetype": "pfb"}
    resp = client.post('/iservice/namespace/name/import', json=payload, headers=good_headers)
    assert resp.status_code == 400

    # if the sam_valid_user fixture changes, this assertion will also need to change
    auditlog = filter(lambda rec: rec.message == "User 123456 hello@bees.com attempted to import from path https://illegal.domains/should/be/logged", caplog.records)
    assert list(auditlog), "Expected audit log message to exist if user specified illegal domain; did not find such message in log."
