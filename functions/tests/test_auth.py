import flask
import pytest
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
    """
    - rawls returns an exception for the workspace, exception reported, sam not called
    - rawls returns no workspace id in the body, ValueError reported, sam not called
    - rawls returns workspace id, action is read, sam not called
    - rawls returns workspace id, action is write, sam called, returns non-OK status
    - rawls returns workspace id, action is write, sam called, returns bad json
    - rawls returns workspace id, action is write, sam called, returns false
    - rawls returns workspace id, action is write, sam called, returns true
    """
    pass
