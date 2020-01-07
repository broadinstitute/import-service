import pytest

from app.auth import userinfo
from app.db.model import *
from app.tests import testutils

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
    import_id = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers).get_data(as_text=True)

    resp = client.get('/iservice/namespace/name/import/{}'.format(import_id), headers=good_headers)
    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == str({"id": import_id, "status": ImportStatus.Pending.name})


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_get_all(client):
    import_id = client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers).get_data(as_text=True)

    resp = client.get('/iservice/namespace/name/import', headers=good_headers)
    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == str([{"id": import_id, "status": ImportStatus.Pending.name}])


@pytest.mark.usefixtures(sam_valid_user, user_has_ws_access, pubsub_publish, "pubsub_fake_env")
def test_get_all_running(client):
    client.post('/iservice/namespace/name/import', json=good_json, headers=good_headers)

    resp = client.get('/iservice/namespace/name/import?running_only', headers=good_headers)
    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == str([])

