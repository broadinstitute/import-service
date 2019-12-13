import logging
import os
import traceback

import flask
from typing import Optional

import google.auth
from google.auth.transport import requests as grequests
from google.oauth2 import id_token

from app.common.exceptions import BadPubSubTokenException


def get_app_token() -> str:
    """Get a token for the application SA using the Application Default Credentials available to the GAE VM."""
    app_sa_creds: google.auth.Credentials
    project: Optional[str]
    app_sa_creds, project = google.auth.default()
    app_sa_creds.refresh()
    return app_sa_creds.token()


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