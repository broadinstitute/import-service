import unittest.mock as mock

import flask
import pytest
from werkzeug.test import EnvironBuilder

import app.external.rawls
from app.auth import user_auth
from app.external.rawls import RawlsWorkspaceResponse
from app.util import exceptions



def fake_authtoken_request(headers: dict) -> flask.Request:
    builder = EnvironBuilder(method='GET', headers=headers)
    env = builder.get_environ()
    return flask.Request(env)


def test_extract_auth_token():
    good_header = {"Authorization": "auth_header", "Some": "Other header"}
    lowercase_header = {"authorization": "auth_header", "Some": "Other header"}
    missing_header = {"Some": "Other header"}
    no_header = {}

    assert user_auth.extract_auth_token(fake_authtoken_request(good_header)) == "auth_header"
    assert user_auth.extract_auth_token(fake_authtoken_request(lowercase_header)) == "auth_header"

    with pytest.raises(exceptions.AuthorizationException):
        user_auth.extract_auth_token(fake_authtoken_request(missing_header))

    with pytest.raises(exceptions.AuthorizationException):
        user_auth.extract_auth_token(fake_authtoken_request(no_header))


def test_workspace_uuid():
    # rawls returns an exception for the workspace, exception raised, auth check not called
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", side_effect = exceptions.ISvcException("bork", 400)):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            with pytest.raises(exceptions.ISvcException):
                user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "read")
            mock_rawls_getaction.assert_not_called()

    # rawls returns no workspace id in the body, KeyError reported, auth check not called
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", side_effect = KeyError):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            with pytest.raises(KeyError):
                user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "read")
            mock_rawls_getaction.assert_not_called()

    # rawls returns workspace id, action is "read", no need to do auth check
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", return_value = RawlsWorkspaceResponse("the-uuid", "proj", "gcp")):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            assert user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "read") == RawlsWorkspaceResponse("the-uuid", "proj", "gcp")
            mock_rawls_getaction.assert_not_called()

    # rawls returns workspace id, action is write, auth check called, returns non-OK status
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", return_value = RawlsWorkspaceResponse("the-uuid", "proj", "gcp")):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action", side_effect = exceptions.ISvcException("bork", 500)) as mock_rawls_getaction:
            with pytest.raises(exceptions.ISvcException):
                user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "write")
            mock_rawls_getaction.assert_called_once()

    # rawls returns workspace id, action is write, auth check called, returns false: you can't do this action
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", return_value = RawlsWorkspaceResponse("the-uuid", "proj", "gcp")):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action", return_value = False) as mock_rawls_getaction:
            with pytest.raises(exceptions.AuthorizationException):
                user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "write")
            mock_rawls_getaction.assert_called_once()

    # rawls returns workspace id, action is write, auth check called, returns true: hooray!
    with mock.patch.object(app.external.rawls, "get_rawls_workspace_info", return_value = RawlsWorkspaceResponse("the-uuid", "proj", "gcp")):
        with mock.patch.object(app.external.rawls, "check_workspace_iam_action", return_value = True) as mock_rawls_getaction:
            assert user_auth.workspace_uuid_and_project_with_auth("wsns", "wsn", "bearer", "write") == RawlsWorkspaceResponse("the-uuid", "proj", "gcp")
            mock_rawls_getaction.assert_called_once()
