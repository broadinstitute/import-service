import pytest
import unittest.mock as mock
from ..common import rawls
from ..common import exceptions


def test_get_workspace_uuid():
    # rawls returns non-OK response
    with mock.patch("functions.common.auth.rawls.requests.get") as mock_get:
        mock_get.return_value.ok = False
        mock_get.return_value.text = "bees"
        mock_get.return_value.status_code = 403
        with pytest.raises(exceptions.ISvcException):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok response with dodgy json
    with mock.patch("functions.common.auth.rawls.requests.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = {"workspaec" : {"workspace_id" : "uuid"}}
        mock_get.return_value.status_code = 200
        with pytest.raises(KeyError):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok with good json, parse it out
    with mock.patch("functions.common.auth.rawls.requests.get") as mock_get:
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = {"workspace" : {"workspaceId" : "uuid"}}
        mock_get.return_value.status_code = 200
        assert rawls.get_workspace_uuid("a", "a", "a") == "uuid"
