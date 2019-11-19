import jsonschema
import pytest
import unittest.mock as mock
from . import testutils
from ..common import sam
from ..common import exceptions


def test_get_user_action_on_resource():
    # sam returns non-OK response
    with testutils.patch_request("functions.common.sam", "get", 403, "bees"):
        with pytest.raises(exceptions.ISvcException):
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

    # sam returns ok response with non-bool response (shouldn't get here irl!)
    with testutils.patch_request("functions.common.sam", "get", 200, json="notabool"):
        with pytest.raises(jsonschema.ValidationError):
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

    # sam returns ok with good json, parse it out
    with testutils.patch_request("functions.common.sam", "get", 200, json=True):
        assert sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")


def test_validate_user():
    # sam returns surprising garbage
    with testutils.patch_request("functions.common.sam", "get", 200, json={"userSubjectID": "foo"}):  # missing a bunch of stuff
        with pytest.raises(jsonschema.ValidationError):
            sam.validate_user("ya29.bearer_token")
