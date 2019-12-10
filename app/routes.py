import flask
import json
import base64
from app.common.httputils import httpify_excs


routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/iservice/<path:rest>', methods=["POST"])
@httpify_excs
def iservice() -> flask.Response:
    from app import service  # scope this import so it's not dragged in for other functions
    """HTTP function for accepting an import request"""
    return flask.make_response(service.handle(flask.request))

# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
def taskchunk() -> flask.Response:

    #TODO: steal from here
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/appengine/standard_python37/pubsub/main.py

    envelope = json.loads(flask.request.data.decode('utf-8'))
    attributes = envelope['message']['attributes']

    from app import chunk_task
    chunk_task.handle(attributes)
    return flask.make_response("ok")
