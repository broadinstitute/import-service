import flask
import jsonschema
import pytest
import unittest.mock as mock
from werkzeug.test import EnvironBuilder
from ..common import auth
from ..common import exceptions


def fake_request(headers: dict) -> flask.Request:
    builder = EnvironBuilder(method='GET', headers=headers)
    env = builder.get_environ()
    return flask.Request(env)


def test_extract_auth_token():
    good_header = {"Authorization": "auth_header", "Some": "Other header"}
    lowercase_header = {"authorization": "auth_header", "Some": "Other header"}
    missing_header = {"Some": "Other header"}
    no_header = {}

    assert auth.extract_auth_token(fake_request(good_header)) == "auth_header"
    assert auth.extract_auth_token(fake_request(lowercase_header)) == "auth_header"

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_auth_token(fake_request(missing_header))

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_auth_token(fake_request(no_header))


def test_workspace_uuid():
    # rawls returns an exception for the workspace, exception raised, sam not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = exceptions.ISvcException("bork", 400)):
        with mock.patch.object(auth.sam, "get_user_action_on_resource") as mock_sam_getaction:
            with pytest.raises(exceptions.ISvcException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read")
            mock_sam_getaction.assert_not_called()

    # rawls returns no workspace id in the body, KeyError reported, sam not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = KeyError):
        with mock.patch.object(auth.sam, "get_user_action_on_resource") as mock_sam_getaction:
            with pytest.raises(KeyError):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read")
            mock_sam_getaction.assert_not_called()

    # rawls returns workspace id, action is "read", no need to call sam
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.sam, "get_user_action_on_resource") as mock_sam_getaction:
            assert auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read") == "uuid"
            mock_sam_getaction.assert_not_called()

    # rawls returns workspace id, action is write, sam called, returns non-OK status
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.sam, "get_user_action_on_resource", side_effect = exceptions.ISvcException("bork", 400)) as mock_sam_getaction:
            with pytest.raises(exceptions.ISvcException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write")
            mock_sam_getaction.assert_called_once()

    # rawls returns workspace id, action is write, sam called, returns bad json
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.sam, "get_user_action_on_resource", side_effect = jsonschema.ValidationError("bad json")) as mock_sam_getaction:
            with pytest.raises(jsonschema.ValidationError):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write")
            mock_sam_getaction.assert_called_once()

    # rawls returns workspace id, action is write, sam called, sam returns false: you can't do this action
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.sam, "get_user_action_on_resource", return_value = False) as mock_sam_getaction:
            with pytest.raises(exceptions.AuthorizationException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write")
            mock_sam_getaction.assert_called_once()

    # rawls returns workspace id, action is write, sam called, sam returns true: hooray!
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.sam, "get_user_action_on_resource", return_value = True) as mock_sam_getaction:
            assert auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write") == "uuid"
            mock_sam_getaction.assert_called_once()

