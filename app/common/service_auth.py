import datetime
import logging
import os
import traceback

import flask

import google.auth
from google.auth.transport import requests as grequests
from google.oauth2 import id_token
import googleapiclient.discovery
from typing import Optional

from app.common.exceptions import BadPubSubTokenException


IMPORT_SERVICE_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile"
]


_cached_isvc_token: Optional[str] = None
_cached_isvc_expiry: Optional[datetime.datetime] = None


def get_isvc_token() -> str:
    """Use the cached token if it still exists and we have at least 5 minutes until it expires."""
    if _cached_isvc_token is not None and \
            _cached_isvc_expiry > datetime.datetime.utcnow() + datetime.timedelta(minutes=5):
        logging.info("using cached token for import service SA")
        return _cached_isvc_token
    else:
        logging.info("generating new token for import service SA")
        return _update_isvc_token()


def _update_isvc_token() -> str:
    """The app engine SA has token creator on the import service SA"""
    credentials, project = google.auth.default()
    iam = googleapiclient.discovery.build('iamcredentials', 'v1', credentials=credentials)

    # create service account name
    email = os.environ.get('IMPORT_SVC_SA_EMAIL')
    name = 'projects/-/serviceAccounts/{}'.format(email)

    # create body for request
    body = {
        'scope': IMPORT_SERVICE_SCOPES
    }

    token_response = iam.projects().serviceAccounts().generateAccessToken(
        name=name,
        body=body,
    ).execute()

    global _cached_isvc_expiry, _cached_isvc_token
    _cached_isvc_token = token_response["accessToken"]
    _cached_isvc_expiry = datetime.datetime.strptime(token_response["expireTime"], "%Y-%m-%dT%H:%M:%SZ")

    return _cached_isvc_token


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