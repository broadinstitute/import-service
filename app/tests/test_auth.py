import unittest.mock as mock

import flask
import pytest
import time
import json
from werkzeug.test import EnvironBuilder
from google.auth import transport as gtransport  # type: ignore

from ..common import auth
from ..common import exceptions


def fake_jwt_request(pubsub_token: str = "token", audience: str = "aud", service_account: str = "sa@sa.org") -> flask.Request:
    mockrq = mock.MagicMock()
    mockrq.args = {"token": pubsub_token}

    payload = {
        "aud": audience,
        "email": service_account,
        "sub": service_account,
        "iss": "https://accounts.google.com",
        "exp": int(time.time()) + 3600,
        "iat": int(time.time())
    }

    mockrq.headers = {"Authorization": f"Bearer {json.dumps(payload)}"}
    return mockrq


def fake_verify_oauth2_token(token: str, request: gtransport.Request, audience: str) -> dict:
    """This function works in concert with fake_jwt_request and the jwt_env fixture to fake behaviour of the
    oauth2 ID token verification, since it's hard to hand-roll a custom ID token that Google's lib likes."""
    claim = json.loads(token)
    if claim["aud"] != audience:
        raise ValueError("Token has wrong audience")
    return claim


@pytest.fixture(scope="function")
def jwt_env(monkeypatch):
    monkeypatch.setenv("PUBSUB_TOKEN", "token")
    monkeypatch.setenv("PUBSUB_AUDIENCE", "aud")
    monkeypatch.setenv("PUBSUB_ACCOUNT", "sa@sa.org")
    monkeypatch.setattr(auth.id_token, "verify_oauth2_token", fake_verify_oauth2_token)


def test_verify_pubsub_jwt(jwt_env):
    good_rq = fake_jwt_request()
    assert auth.verify_pubsub_jwt(good_rq) is None

    wrong_token = fake_jwt_request(pubsub_token="wrong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        auth.verify_pubsub_jwt(wrong_token)

    wrong_audience = fake_jwt_request(audience="wrong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        auth.verify_pubsub_jwt(wrong_audience)

    wrong_sa = fake_jwt_request(service_account="wrong@wr.ong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        auth.verify_pubsub_jwt(wrong_sa)


def fake_authtoken_request(headers: dict) -> flask.Request:
    builder = EnvironBuilder(method='GET', headers=headers)
    env = builder.get_environ()
    return flask.Request(env)


def test_extract_auth_token():
    good_header = {"Authorization": "auth_header", "Some": "Other header"}
    lowercase_header = {"authorization": "auth_header", "Some": "Other header"}
    missing_header = {"Some": "Other header"}
    no_header = {}

    assert auth.extract_auth_token(fake_authtoken_request(good_header)) == "auth_header"
    assert auth.extract_auth_token(fake_authtoken_request(lowercase_header)) == "auth_header"

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_auth_token(fake_authtoken_request(missing_header))

    with pytest.raises(exceptions.AuthorizationException):
        auth.extract_auth_token(fake_authtoken_request(no_header))


def test_workspace_uuid():
    # rawls returns an exception for the workspace, exception raised, auth check not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = exceptions.ISvcException("bork", 400)):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            with pytest.raises(exceptions.ISvcException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read")
            mock_rawls_getaction.assert_not_called()

    # rawls returns no workspace id in the body, KeyError reported, auth check not called
    with mock.patch.object(auth.rawls, "get_workspace_uuid", side_effect = KeyError):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            with pytest.raises(KeyError):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read")
            mock_rawls_getaction.assert_not_called()

    # rawls returns workspace id, action is "read", no need to do auth check
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action") as mock_rawls_getaction:
            assert auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "read") == "uuid"
            mock_rawls_getaction.assert_not_called()

    # rawls returns workspace id, action is write, auth check called, returns non-OK status
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action", side_effect = exceptions.ISvcException("bork", 500)) as mock_rawls_getaction:
            with pytest.raises(exceptions.ISvcException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write")
            mock_rawls_getaction.assert_called_once()

    # rawls returns workspace id, action is write, auth check called, returns false: you can't do this action
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action", return_value = False) as mock_rawls_getaction:
            with pytest.raises(exceptions.AuthorizationException):
                auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write")
            mock_rawls_getaction.assert_called_once()

    # rawls returns workspace id, action is write, auth check called, returns true: hooray!
    with mock.patch.object(auth.rawls, "get_workspace_uuid", return_value = "uuid"):
        with mock.patch.object(auth.rawls, "check_workspace_iam_action", return_value = True) as mock_rawls_getaction:
            assert auth.workspace_uuid_with_auth("wsns", "wsn", "bearer", "write") == "uuid"
            mock_rawls_getaction.assert_called_once()

