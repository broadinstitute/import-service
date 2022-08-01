import jsonschema
import pytest
import unittest.mock as mock

from app.tests import testutils
from app.util import exceptions
from app.external import sam
from app.auth import userinfo


def test_validate_user():
    # sam returns an error
    with testutils.patch_request("app.external.sam", "get", 403):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns the wrong json
    with testutils.patch_request("app.external.sam", "get", 200, json={"userSubjectID": "foo"}):  # missing a bunch of stuff
        with pytest.raises(jsonschema.ValidationError):
            sam.validate_user("ya29.bearer_token")

    # sam doesn't know who you are
    # note here that sam returns 404 for "who are you", and we return 403 "you're not authorized"
    with testutils.patch_request("app.external.sam", "get", 404):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns an error
    with testutils.patch_request("app.external.sam", "get", 500):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 500

    # sam returns a non-enabled user
    bad_user = {"userSubjectId": "12456", "userEmail": "hello@bees.com", "enabled": False}
    with testutils.patch_request("app.external.sam", "get", 200, json=bad_user):
        with pytest.raises(exceptions.AuthorizationException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns a good user
    good_user = {"userSubjectId": "123456", "userEmail": "hello@bees.com", "enabled": True}
    with testutils.patch_request("app.external.sam", "get", 200, json=good_user):
        assert sam.validate_user("ya29.bearer_token") == userinfo.UserInfo("123456", "hello@bees.com", True)


def test_get_user_action_on_resource():
    # sam returns non-OK response
    with testutils.patch_request("app.external.sam", "get", 403, "not ok"):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.get_user_action_on_resource("rtype", "resource_id", "action", "bearer")
            assert excinfo.value.http_status == 403

    # sam returns ok response with non-bool response (shouldn't get here irl!)
    with testutils.patch_request("app.external.sam", "get", 200, json="notabool"):
        with pytest.raises(jsonschema.ValidationError):
            sam.get_user_action_on_resource("rtype", "resource_id", "action", "bearer")

    # sam returns ok with good json, parse it out
    with testutils.patch_request("app.external.sam", "get", 200, json=True):
        assert sam.get_user_action_on_resource("rtype", "resource_id", "action", "bearer")


def test_add_child_policy_member():
    # tdr returns an false
    with testutils.patch_request("app.external.sam", "put", 403):
        with pytest.raises(exceptions.AuthorizationException) as excinfo:
            sam.add_child_policy_member("datasnapshot", "snapshot_id", "reader", "workspace",
                "workspace_id", "writer", "ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # tdr returns true
    with testutils.patch_request("app.external.sam", "put", 200):
        sam.add_child_policy_member("datasnapshot", "snapshot_id", "reader", "workspace",
            "workspace_id", "writer", "ya29.bearer_token")


@pytest.mark.usefixtures(
    testutils.fxpatch(
        "app.auth.service_auth.get_isvc_token",
        return_value={"accessToken": "ya29.isvc_token", "expireTime": "2014-10-02T15:01:23Z"}))
def test_admin_get_pet_token():
    # happy path
    with testutils.patch_request("app.external.sam", "get", 200, json={}):
        with mock.patch("app.external.sam._creds_from_key") as mock_pet_creds:
            mock_pet_creds.return_value.token = "ya29.pet_token"
            assert sam.admin_get_pet_token("project", "user@hello.com") == "ya29.pet_token"

    # sam returns non-OK response
    with testutils.patch_request("app.external.sam", "get", 404, "who?"):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.admin_get_pet_token("project", "user@hello.com")
            assert excinfo.value.http_status == 404
