import unittest.mock as mock

import datetime
import flask
import pytest
import time
import json
from google.auth import transport as gtransport

from app.auth import service_auth
from app.util import exceptions
from app.tests import testutils


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
    monkeypatch.setattr(service_auth.id_token, "verify_oauth2_token", fake_verify_oauth2_token)


def test_verify_pubsub_jwt(jwt_env):
    good_rq = fake_jwt_request()
    assert service_auth.verify_pubsub_jwt(good_rq) is None

    wrong_token = fake_jwt_request(pubsub_token="wrong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        service_auth.verify_pubsub_jwt(wrong_token)

    wrong_audience = fake_jwt_request(audience="wrong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        service_auth.verify_pubsub_jwt(wrong_audience)

    wrong_sa = fake_jwt_request(service_account="wrong@wr.ong")
    with pytest.raises(exceptions.BadPubSubTokenException):
        service_auth.verify_pubsub_jwt(wrong_sa)


@pytest.mark.usefixtures(
    testutils.fxpatch(
        "app.auth.service_auth._get_isvc_token_from_google",
        return_value = {"accessToken": "ya29.google", "expireTime": "2014-10-02T15:01:23Z"}))
def test_get_isvc_token():
    # test we call google if there's no cache (which there isn't by default)
    assert service_auth.get_isvc_token() == "ya29.google"

    # test we call google if the cache is expired
    with mock.patch("app.auth.service_auth._cached_isvc_token",
                    service_auth.TokenAndExpiry(token="ya29.cached", expiry=datetime.datetime.utcnow() - datetime.timedelta(hours=1))):
        assert service_auth.get_isvc_token() == "ya29.google"

    # test we use the cache if it's still okay
    with mock.patch("app.auth.service_auth._cached_isvc_token",
                    service_auth.TokenAndExpiry(token="ya29.cached", expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1))):
        assert service_auth.get_isvc_token() == "ya29.cached"
