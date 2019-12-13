import flask.testing
import jsonschema
import pytest

from . import testutils
from .. import service
from ..common import db, exceptions, userinfo
from ..common.model import *


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


good_json = {"path": "foo", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah"}

sam_valid_user = testutils.fxpatch(
    "app.common.sam.validate_user",
    return_value=userinfo.UserInfo("123456", "hello@bees.com", True))

user_has_ws_access = testutils.fxpatch(
    "app.common.auth.workspace_uuid_with_auth",
    return_value="some-uuid")


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_golden_path(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_wrong_httpmethod(client: flask.testing.FlaskClient):
    resp = client.get('/iservice/namespace/name/import', headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_wrong_path(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/import')
    assert resp.status_code == 404


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_missing_json(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/namespace/name/import', headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_not_json(client):
    resp = client.post('/iservice/namespace/name/import', data="not a json object", headers=good_headers)
    assert resp.status_code == 400


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_bad_json(client):
    resp = client.post('/iservice/namespace/name/import', json={"bees":"buzz"}, headers=good_headers)
    assert resp.status_code == 400


def test_bad_token(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers={"Unauthorization?!": "ohno"})
    assert resp.status_code == 403


@pytest.mark.usefixtures(
    testutils.fxpatch(
        "app.common.sam.validate_user",
        side_effect = exceptions.ISvcException("who are you?", 404)))
def test_user_not_found(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    sam_valid_user,
    testutils.fxpatch(
        "app.common.auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("what workspace?", 404)))
def test_user_cant_see_workspace(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    sam_valid_user,
    testutils.fxpatch(
        "app.common.auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("you can't write to this", 403)))
def test_user_cant_write_to_workspace(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 403