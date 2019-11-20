import pytest

from . import testutils
from ..common import exceptions
from ..common import rawls


def test_get_workspace_uuid():
    # rawls returns non-OK response
    with testutils.patch_request("functions.common.rawls", "get", status_code = 403, text="no"):
        with pytest.raises(exceptions.ISvcException):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok response with dodgy json
    with testutils.patch_request("functions.common.rawls", "get", status_code = 200, json={"spacework" : {"wOrKsPaCeId" : "uuid"}}):
        with pytest.raises(KeyError):
            rawls.get_workspace_uuid("a", "a", "a")

    # rawls returns ok with good json, parse it out
    with testutils.patch_request("functions.common.rawls", "get", status_code = 200, json={"workspace" : {"workspaceId" : "uuid"}}):
        assert rawls.get_workspace_uuid("a", "a", "a") == "uuid"

