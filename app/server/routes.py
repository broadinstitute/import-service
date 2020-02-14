import flask
import json
import humps
from typing import Dict, Callable

from app import new_import, translate, status
import app.auth.service_auth
from app.server.requestutils import httpify_excs, pubsubify_excs

routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/<workspace_namespace>/<workspace_name>/imports', methods=["POST"])
@httpify_excs
def create_import(workspace_namespace, workspace_name) -> flask.Response:
    """Accept an import request.
    ---
    parameters:
      - name: workspace_namespace
        in: path
        schema:
          type: string
        required: true
      - name: workspace_name
        in: path
        schema:
          type: string
        required: true
      - name: import_request
        in: body
        schema:
          $ref: '#/definitions/import_request'
    responses:
      201:
        description: it's all good
    """
    return new_import.handle(flask.request, workspace_namespace, workspace_name)


@routes.route('/<workspace_namespace>/<workspace_name>/imports/<import_id>', methods=["GET"])
@httpify_excs
def import_status(workspace_namespace, workspace_name, import_id) -> flask.Response:
    """Return the status of an import job.
    ---
    parameters:
      - name: workspace_namespace
        in: path
        schema:
          type: string
        required: true
      - name: workspace_name
        in: path
        schema:
          type: string
        required: true
      - name: import_id
        in: path
        schema:
          type: string
    responses:
      200:
        description: here's the status
        content:
          application/json:
            schema:
              $ref: '#/definitions/import_status'
    """
    return status.handle_get_import_status(flask.request, workspace_namespace, workspace_name, import_id)


@routes.route('/<ws_ns>/<ws_name>/imports', methods=["GET"])
@httpify_excs
def import_status_workspace(ws_ns, ws_name) -> flask.Response:
    """Return the status of import jobs in a workspace"""
    return status.handle_list_import_status(flask.request, ws_ns, ws_name)


# Dispatcher for pubsub messages.
pubsub_dispatch: Dict[str, Callable[[Dict[str, str]], flask.Response]] = {
    "translate": translate.handle,
    "status": status.external_update_status
}


# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
@pubsubify_excs
def pubsub_receive() -> flask.Response:
    app.auth.service_auth.verify_pubsub_jwt(flask.request)

    envelope = json.loads(flask.request.data.decode('utf-8'))
    attributes = envelope['message']['attributes']

    # humps.decamelize turns camelCase to snake_case in dict keys
    return pubsub_dispatch[attributes["action"]](humps.decamelize(attributes))
