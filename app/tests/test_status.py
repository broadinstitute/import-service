import flask.testing
import json
import jsonschema
import pytest

from app.tests import testutils
from app import service, status
from app.util import exceptions
from app.db import db
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


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_get_one(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))

    resp2 = client.get('/iservice/namespace/name/import/{}'.format(resp.get_data(as_text=True)), headers=good_headers)
    assert resp2.status_code == 200
    assert resp2.get_data(as_text=True) == ImportStatus.Pending.name


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_get_all(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    import_id = dbres[0].id
    assert len(dbres) == 1
    assert import_id == str(resp.get_data(as_text=True))

    resp2 = client.get('/iservice/namespace/name/import', headers=good_headers)
    assert resp2.status_code == 200
    assert resp2.get_data(as_text=True) == str([{"id": import_id, "status": ImportStatus.Pending.name}])


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_get_all_running(client):
    resp = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    import_id = dbres[0].id
    assert len(dbres) == 1
    assert import_id == str(resp.get_data(as_text=True))

    resp2 = client.get('/iservice/namespace/name/import?running_only', headers=good_headers)
    assert resp2.status_code == 200
    assert resp2.get_data(as_text=True) == str([])

