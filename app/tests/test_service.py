import flask.testing
import jsonschema
import pytest

from app.tests import testutils
from app import service, translate
from app.util import exceptions
from app.db import db
from app.auth import userinfo
from app.db.model import *


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


good_json = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah"}

sam_valid_user = testutils.fxpatch(
    "app.external.sam.validate_user",
    return_value=userinfo.UserInfo("123456", "hello@bees.com", True))

user_has_ws_access = testutils.fxpatch(
    "app.auth.user_auth.workspace_uuid_with_auth",
    return_value="some-uuid")

# replace the publish to google pub/sub with a no-op one
pubsub_publish = testutils.fxpatch(
    "app.external.pubsub.publish_self")


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_golden_path(client):
    resp = client.post('/iservice/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_wrong_path(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/imports')
    assert resp.status_code == 404


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_missing_json(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/namespace/name/imports', headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_not_json(client):
    resp = client.post('/iservice/namespace/name/imports', data="not a json object", headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_bad_json(client):
    resp = client.post('/iservice/namespace/name/imports', json={"bees":"buzz"}, headers=good_headers)
    assert resp.status_code == 400


def test_bad_token(client):
    resp = client.post('/iservice/namespace/name/imports', json=good_json, headers={"Unauthorization?!": "ohno"})
    assert resp.status_code == 403


@pytest.mark.usefixtures(
    testutils.fxpatch(
        "app.external.sam.validate_user",
        side_effect = exceptions.ISvcException("who are you?", 404)))
def test_user_not_found(client):
    resp = client.post('/iservice/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    sam_valid_user,
    testutils.fxpatch(
        "app.auth.user_auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("what workspace?", 404)))
def test_user_cant_see_workspace(client):
    resp = client.post('/iservice/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    sam_valid_user,
    testutils.fxpatch(
        "app.auth.user_auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("you can't write to this", 403)))
def test_user_cant_write_to_workspace(client):
    resp = client.post('/iservice/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 403
