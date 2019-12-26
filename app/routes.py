import flask
import json
import os

import app.common.service_auth
from app.common.httputils import httpify_excs


routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/iservice/<path:rest>', methods=["POST"])
@httpify_excs
def iservice(rest) -> flask.Response:
    from app import service  # scope this import so it's not dragged in for other functions
    """HTTP function for accepting an import request"""
    return flask.make_response(service.handle(flask.request))


# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
@httpify_excs
def taskchunk() -> flask.Response:
    app.common.service_auth.verify_pubsub_jwt(flask.request)

    envelope = json.loads(flask.request.data.decode('utf-8'))
    attributes = envelope['message']['attributes']

    from app import chunk_task
    chunk_task.handle(attributes)
    return flask.make_response("ok")
