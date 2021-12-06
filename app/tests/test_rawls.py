import pytest

from app.tests import testutils
from app.util import exceptions
from app.external import rawls
from app.external.rawls import RawlsWorkspaceResponse

def test_get_workspace_uuid():
    # rawls returns non-OK response
    with testutils.patch_request("app.external.rawls", "get", status_code = 403, text="no"):
        with pytest.raises(exceptions.ISvcException):
            rawls.get_workspace_uuid_and_project("a", "a", "a")

    # rawls returns ok response with dodgy json
    with testutils.patch_request("app.external.rawls", "get", status_code = 200, json={"spacework" : {"wOrKsPaCeId" : "uuid", "googleProject": "proj"}}):
        with pytest.raises(KeyError):
            rawls.get_workspace_uuid_and_project("a", "a", "a")

    # rawls returns ok with good json, parse it out
    with testutils.patch_request("app.external.rawls", "get", status_code = 200, json={"workspace" : {"workspaceId" : "the-uuid", "googleProject": "proj"}}):
        assert rawls.get_workspace_uuid_and_project("a", "a", "a") == RawlsWorkspaceResponse("the-uuid", "proj")


def test_check_workspace_iam_action():
    # rawls says yes
    with testutils.patch_request("app.external.rawls", "get", status_code = 204):
        assert rawls.check_workspace_iam_action("a", "a", "a", "a")

    # rawls says no
    with testutils.patch_request("app.external.rawls", "get", status_code = 403):
        assert not rawls.check_workspace_iam_action("a", "a", "a", "a")

    # rawls errors
    with testutils.patch_request("app.external.rawls", "get", status_code = 500, text="barf"):
        with pytest.raises(exceptions.ISvcException):
            rawls.check_workspace_iam_action("a", "a", "a", "a")
