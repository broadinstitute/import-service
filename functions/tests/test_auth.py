import flask
import pytest
import unittest.mock as mock
from werkzeug.test import EnvironBuilder
from ..common import auth
from ..common import exceptions


def fake_request(headers: dict) -> flask.Request:
    builder = EnvironBuilder(method='GET', headers=headers)
    env = builder.get_environ()
    return flask.Request(env)


def test_extract_bearer_token():
    good_header = {"Authorization": "auth_header", "Some": "Other header"}
    lowercase_header = {"authorization": "auth_header", "Some": "Other header"}
    missing_header = {"Some": "Other header"}
    no_header = {}

    assert auth.extract_bearer_token(fake_request(good_header)) == "auth_header"
    assert auth.extract_bearer_token(fake_request(lowercase_header)) == "auth_header"

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_bearer_token(fake_request(missing_header))

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_bearer_token(fake_request(no_header))


def test_workspace_uuid():
    # rawls returns an exception for the workspace, exception raised, sam not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = exceptions.ISvcException("bork", 400)):
        auth.sam.get_user_action_on_resource: mock.MagicMock = mock.MagicMock()
        with pytest.raises(exceptions.ISvcException):
            auth.workspace_uuid_with_auth("a", "a", "a", "read")
        auth.sam.get_user_action_on_resource.assert_not_called()

    # rawls returns no workspace id in the body, KeyError reported, sam not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = KeyError):
        auth.sam.get_user_action_on_resource: mock.MagicMock = mock.MagicMock()
        with pytest.raises(KeyError):
            auth.workspace_uuid_with_auth("a", "a", "a", "read")
        auth.sam.get_user_action_on_resource.assert_not_called()

    # rawls returns workspace id, action is "read", no need to call sam
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        auth.sam.get_user_action_on_resource: mock.MagicMock = mock.MagicMock()
        assert auth.workspace_uuid_with_auth("a", "a", "a", "read") == "uuid"
        auth.sam.get_user_action_on_resource.assert_not_called()



"""
    - rawls returns workspace id, action is write, sam called, returns non-OK status
    - rawls returns workspace id, action is write, sam called, returns bad json
    - rawls returns workspace id, action is write, sam called, returns false
    - rawls returns workspace id, action is write, sam called, returns true
    """
