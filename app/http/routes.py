import flask
import json
from typing import Dict, Callable

from app import service, translate, status
import app.auth.service_auth
from app.http.httputils import httpify_excs

routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/iservice/<path:rest>', methods=["POST"])
@httpify_excs
def iservice(rest) -> flask.Response:
    """Accept an import request"""
    return flask.make_response(service.handle(flask.request))


@routes.route('/iservice/<ws_ns>/<ws_name>/import/<import_id>', methods=["GET"])
@httpify_excs
def import_status(ws_ns, ws_name, import_id) -> flask.Response:
    """Return the status of an import job"""
    return flask.make_response(status.handle_one(flask.request, ws_ns, ws_name, import_id))


@routes.route('/iservice/<ws_ns>/<ws_name>/import', methods=["GET"])
@httpify_excs
def import_status_workspace(ws_ns, ws_name) -> flask.Response:
    """Return the status of import jobs in a workspace"""
    return flask.make_response(status.handle_list(flask.request, ws_ns, ws_name))


# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
@httpify_excs
def pubsub_receive() -> flask.Response:
    app.auth.service_auth.verify_pubsub_jwt(flask.request)

    envelope = json.loads(flask.request.data.decode('utf-8'))
    attributes = envelope['message']['attributes']

    return route_pubsub(attributes["action"], attributes)


def route_pubsub(action: str, attributes: Dict[str, str]) -> flask.Response:
    """Dispatcher for pubsub messages."""
    DISPATCH_LOOKUP: Dict[str, Callable[[Dict[str, str]], flask.Response]] = {
        "translate": translate.translate
    }

    return DISPATCH_LOOKUP[action](attributes)
