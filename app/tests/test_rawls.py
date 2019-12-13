import pytest

from . import testutils
from ..common import exceptions
from ..common import rawls


def test_get_workspace_uuid():
    # rawls returns non-OK response
    with testutils.patch_request("app.common.rawls", "get", status_code = 403, text="no"):
        with pytest.raises(exceptions.ISvcException):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok response with dodgy json
    with testutils.patch_request("app.common.rawls", "get", status_code = 200, json={"spacework" : {"wOrKsPaCeId" : "uuid"}}):
        with pytest.raises(KeyError):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok with good json, parse it out
    with testutils.patch_request("app.common.rawls", "get", status_code = 200, json={"workspace" : {"workspaceId" : "uuid"}}):
        assert rawls.get_workspace_uuid("a", "a", "a") == "uuid"


def test_check_workspace_iam_action():
    # rawls says yes
    with testutils.patch_request("app.common.rawls", "get", status_code = 204):
        assert rawls.check_workspace_iam_action("a", "a", "a", "a")

    # rawls says no
    with testutils.patch_request("app.common.rawls", "get", status_code = 403):
        assert not rawls.check_workspace_iam_action("a", "a", "a", "a")

    # rawls errors
    with testutils.patch_request("app.common.rawls", "get", status_code = 500, text="barf"):
        with pytest.raises(exceptions.ISvcException):
            rawls.check_workspace_iam_action("a", "a", "a", "a")
