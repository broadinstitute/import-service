import datetime
import logging
import os
import traceback

import flask

import google.auth
from google.auth.transport import requests as grequests
from google.oauth2 import id_token
import googleapiclient.discovery
from typing import Optional, NamedTuple

from app.util.exceptions import BadPubSubTokenException


IMPORT_SERVICE_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile"
]


class TokenAndExpiry(NamedTuple):
    token: str
    expiry: datetime.datetime


_cached_isvc_token: Optional[TokenAndExpiry] = None


def get_isvc_token() -> str:
    """Use the cached token if it still exists and we have at least 5 minutes until it expires."""
    if _cached_isvc_token is not None and \
            _cached_isvc_token.expiry > datetime.datetime.utcnow() + datetime.timedelta(minutes=5):
        logging.info("using cached token for import service SA")
        return _cached_isvc_token.token
    else:
        logging.info("generating new token for import service SA")
        return _update_isvc_token()


def _google_expiretime_to_datetime(expire_time: str) -> datetime.datetime:
    """Google's documentation for the generateAccessToken endpoint describes the expireTime field as a timestamp in
    RFC3339 format, providing the example "2014-10-02T15:01:23.045123456Z" -- i.e. the time all the way to nanoseconds.
    In practice, the endpoint currently omits the nanoseconds entirely. Google verified this behaviour in a support
    ticket, unhelpfully adding "At some point in the future we may start supporting fractional times, and would not
    consider that a breaking change."

    Therefore we need to handle timestamps both with and without nanoseconds. Since this is just a token expiry,
    dropping the nanoseconds component will mean at worse we refresh the token (one second minus one nanosecond) early.

    https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
    https://console.cloud.google.com/support/cases/detail/21652153"""

    # if there are nanoseconds, everything left of the dot will be the time (with no Z, so we put it back).
    # if there aren't nanoseconds, there'll be no dot, so we don't need to reinstate the Z.
    trunc_time = expire_time.split('.')[0]
    if trunc_time[-1] != 'Z':
        trunc_time += 'Z'

    return datetime.datetime.strptime(trunc_time, "%Y-%m-%dT%H:%M:%SZ")


def _update_isvc_token() -> str:
    """The app engine SA has token creator on the import service SA"""
    token_response = _get_isvc_token_from_google()

    global _cached_isvc_token
    _cached_isvc_token = TokenAndExpiry(token=token_response["accessToken"],
                                        expiry=_google_expiretime_to_datetime(token_response["expireTime"]))

    return _cached_isvc_token.token


def _get_isvc_token_from_google() -> dict:
    # create service account name
    email = os.environ.get('IMPORT_SVC_SA_EMAIL')
    name = 'projects/-/serviceAccounts/{}'.format(email)

    # create body for request
    body = {
        'scope': IMPORT_SERVICE_SCOPES
    }

    credentials, project = google.auth.default()
    iam = googleapiclient.discovery.build('iamcredentials', 'v1', credentials=credentials)

    return iam.projects().serviceAccounts().generateAccessToken(
        name=name,
        body=body,
    ).execute()


def verify_pubsub_jwt(request: flask.Request) -> None:
    """Verify that this request came from Cloud Pub/Sub.
    This looks for a secret token in a queryparam, then decodes the Bearer token
    and checks identity and audience.

    See here: https://cloud.google.com/pubsub/docs/push#using_json_web_tokens_jwts"""
    if request.args.get('token', '') != os.environ.get('PUBSUB_TOKEN'):
        logging.info("Bad Pub/Sub token")
        raise BadPubSubTokenException()

    bearer_token = request.headers.get('Authorization', '')
    token = bearer_token.split(' ', maxsplit=1)[1]

    try:
        claim = id_token.verify_oauth2_token(token, grequests.Request(),
                                             audience=os.environ.get('PUBSUB_AUDIENCE'))
        if claim['iss'] not in [
            'accounts.google.com',
            'https://accounts.google.com'
        ]:
            # bad issuer
            logging.info("Bad issuer")
            raise BadPubSubTokenException()

        if claim['email'] != os.environ.get('PUBSUB_ACCOUNT'):
            logging.info("Incorrect email address")
            raise BadPubSubTokenException()
    except Exception as e:
        # eats all exceptions, including ones thrown by verify_oauth2_token if e.g. audience is wrong
        logging.info(traceback.format_exc())
        raise BadPubSubTokenException()