import flask.testing
import jsonschema
import pytest

from app.tests import testutils
from app import new_import, translate
from app.util import exceptions
from app.db import db
from app.db.model import *


good_json = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}
good_headers = {"Authorization": "Bearer ya29.blahblah", "Accept": "application/json"}


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
        "app.auth.user_auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("what workspace?", 404)))
def test_user_cant_see_workspace(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 404


@pytest.mark.usefixtures(
    "sam_valid_user",
    testutils.fxpatch(
        "app.auth.user_auth.workspace_uuid_with_auth",
        side_effect = exceptions.ISvcException("you can't write to this", 403)))
def test_user_cant_write_to_workspace(client):
    resp = client.post('/namespace/name/imports', json=good_json, headers=good_headers)
    assert resp.status_code == 403

@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_defaults_true_when_missing(client):
    json_payload = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb"}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert dbres[0].is_upsert

@pytest.mark.parametrize("input_value", ["false", "False", 0, "something else", "", False])
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_is_false_when_falsey_in_json(input_value, client):
    json_payload = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb", "isUpsert": input_value}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert dbres[0].is_upsert == False

@pytest.mark.parametrize("input_value", ["true", "tRuE", "  TRUE  ", True])
@pytest.mark.usefixtures("sam_valid_user", "user_has_ws_access", "pubsub_publish", "pubsub_fake_env")
def test_is_upsert_is_true_when_truthy_in_json(input_value, client):
    json_payload = {"path": f"https://{translate.VALID_NETLOCS[0]}/some/path", "filetype": "pfb", "isUpsert": input_value}

    resp = client.post('/mynamespace/myname/imports', json=json_payload, headers=good_headers)
    assert resp.status_code == 201

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    id = resp.json["jobId"]
    dbres = sess.query(Import).filter(Import.id == id).all()
    assert len(dbres) == 1
    # assert that the db row contains True for is_upsert
    assert dbres[0].is_upsert
