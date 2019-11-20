import flask.testing
import jsonschema
import unittest.mock as mock
import pytest
from ..common import db, userinfo
from ..common.model import *
from .. import service
from . import testutils


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


good_headers = {"Authorization": "Bearer ya29.blahblah"}
sam_valid_user = testutils.fxpatch("functions.common.sam.validate_user", return_value = userinfo.UserInfo("123456", "hello@bees.com", True))
user_has_ws_access = testutils.fxpatch("functions.common.auth.workspace_uuid_with_auth", return_value="some-uuid")


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_good_json(client):
    resp = client.post('/iservice/namespace/name/import', json={"path": "foo", "filetype": "pfb"}, headers=good_headers)
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access)
def test_wrong_httpmethod(client: flask.testing.FlaskClient):
    resp = client.get('/iservice/namespace/name/import', headers=good_headers)
    assert resp.status_code == 405


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

