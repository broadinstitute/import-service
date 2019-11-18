import jsonschema
import pytest
import unittest.mock as mock
from ..common import sam
from ..common import exceptions


def test_get_boo():
    # sam returns non-OK response
    with mock.patch.object(sam.requests, "get") as mock_get:
        mock_get.return_value.ok = False
        mock_get.return_value.text = "bees"
        mock_get.return_value.status_code = 403
        with pytest.raises(exceptions.ISvcException):
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

    # sam returns ok response with non-bool response (shouldn't get here irl!)
    with mock.patch("functions.common.sam.requests.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = "notabool"
        mock_get.return_value.status_code = 200
        with pytest.raises(jsonschema.ValidationError):
            sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")

    # sam returns ok with good json, parse it out
    with mock.patch("functions.common.sam.requests.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = True
        mock_get.return_value.status_code = 200
        assert sam.get_user_action_on_resource("rtype", "rid", "action", "bearer")
