import jsonschema
import pytest

from . import testutils
from ..common import sam, exceptions, userinfo


def test_validate_user():
    # sam returns an error
    with testutils.patch_request("functions.common.sam", "get", 403):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns the wrong json
    with testutils.patch_request("functions.common.sam", "get", 200, json={"userSubjectID": "foo"}):  # missing a bunch of stuff
        with pytest.raises(jsonschema.ValidationError):
            sam.validate_user("ya29.bearer_token")

    # sam doesn't know who you are
    # note here that sam returns 404 for "who are you", and we return 403 "you're not authorized"
    with testutils.patch_request("functions.common.sam", "get", 404):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns an error
    with testutils.patch_request("functions.common.sam", "get", 500):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 500

    # sam returns a non-enabled user
    bad_user = {"userSubjectId": "12456", "userEmail": "hello@bees.com", "enabled": False}
    with testutils.patch_request("functions.common.sam", "get", 200, json=bad_user):
        with pytest.raises(exceptions.AuthorizationException) as excinfo:
            sam.validate_user("ya29.bearer_token")
            assert excinfo.value.http_status == 403

    # sam returns a good user
    good_user = {"userSubjectId": "123456", "userEmail": "hello@bees.com", "enabled": True}
    with testutils.patch_request("functions.common.sam", "get", 200, json=good_user):
        assert sam.validate_user("ya29.bearer_token") == userinfo.UserInfo("123456", "hello@bees.com", True)


def test_get_user_action_on_resource():
    # sam returns non-OK response
    with testutils.patch_request("functions.common.sam", "get", 403, "bees"):
        with pytest.raises(exceptions.ISvcException) as excinfo:
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")
            assert excinfo.value.http_status == 403

    # sam returns ok response with non-bool response (shouldn't get here irl!)
    with testutils.patch_request("functions.common.sam", "get", 200, json="notabool"):
        with pytest.raises(jsonschema.ValidationError):
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

    # sam returns ok with good json, parse it out
    with testutils.patch_request("functions.common.sam", "get", 200, json=True):
        assert sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

