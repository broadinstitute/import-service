import flask
import json
from typing import Dict, Callable

from app import new_import, translate, status
import app.auth.service_auth
from app.server.requestutils import httpify_excs, pubsubify_excs

routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/<ws_ns>/<ws_name>/imports', methods=["POST"])
@httpify_excs
def create_import(ws_ns, ws_name) -> flask.Response:
    """Accept an import request"""
    return new_import.handle(flask.request, ws_ns, ws_name)


@routes.route('/<ws_ns>/<ws_name>/imports/<import_id>', methods=["GET"])
@httpify_excs
def import_status(ws_ns, ws_name, import_id) -> flask.Response:
    """Return the status of an import job"""
    return status.handle_get_import_status(flask.request, ws_ns, ws_name, import_id)


@routes.route('/<ws_ns>/<ws_name>/imports', methods=["GET"])
@httpify_excs
def import_status_workspace(ws_ns, ws_name) -> flask.Response:
    """Return the status of import jobs in a workspace"""
    return status.handle_list_import_status(flask.request, ws_ns, ws_name)


# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
@pubsubify_excs
def pubsub_receive() -> flask.Response:
    app.auth.service_auth.verify_pubsub_jwt(flask.request)

    envelope = json.loads(flask.request.data.decode('utf-8'))
    attributes = envelope['message']['attributes']

    return route_pubsub(attributes["action"], attributes)


def route_pubsub(action: str, attributes: Dict[str, str]) -> flask.Response:
    """Dispatcher for pubsub messages."""
    DISPATCH_LOOKUP: Dict[str, Callable[[Dict[str, str]], flask.Response]] = {
        "translate": translate.handle,
        "status": status.external_update_status
    }

    return DISPATCH_LOOKUP[action](attributes)
