import flask
from flask_restx import Api, Resource
import json
import humps
from typing import Dict, Callable, Any

from app import new_import, translate, status, health
from app.db import model
import app.auth.service_auth
from app.server.requestutils import httpify_excs, pubsubify_excs

routes = flask.Blueprint('import-service', __name__, '/')
api = Api(routes, version='1.0', title='Import Service',
          description='import service')

ns = api.namespace('/', description='import handling')


import_status_response_model = ns.model("ImportStatusResponse", model.ImportStatusResponse.get_model())


@ns.route('/<ws_ns>/<ws_name>/imports/<import_id>')
class SpecificImport(Resource):
    @httpify_excs
    @ns.marshal_with(import_status_response_model)
    def get(self, ws_ns, ws_name, import_id):
        """Return status for this import."""
        return status.handle_get_import_status(flask.request, ws_ns, ws_name, import_id)


@ns.route('/<ws_ns>/<ws_name>/imports')
class Imports(Resource):
    @httpify_excs
    @ns.marshal_with(import_status_response_model, 201)
    def post(self, ws_ns, ws_name):
        """Accept an import request."""
        return new_import.handle(flask.request, ws_ns, ws_name), 201

    @httpify_excs
    @ns.marshal_with(import_status_response_model, 200)
    def get(self, ws_ns, ws_name):
        """Return all imports in the workspace."""
        return status.handle_list_import_status(flask.request, ws_ns, ws_name)


@routes.route('/health', methods=["GET"])
@httpify_excs
def health_check() -> flask.Response:
    return health.handle_health_check()


# Dispatcher for pubsub messages.
pubsub_dispatch: Dict[str, Callable[[Dict[str, str]], Any]] = {
    "translate": translate.handle,
    "status": status.external_update_status
}


# This particular URL, though weird, can be secured using GCP magic.
# See https://cloud.google.com/pubsub/docs/push#authenticating_standard_and_urls
@ns.route('/_ah/push-handlers/receive_messages', doc=False)
class PubSub(Resource):
    @pubsubify_excs
    @ns.marshal_with(import_status_response_model, 200)
    def post(self) -> flask.Response:
        app.auth.service_auth.verify_pubsub_jwt(flask.request)

        envelope = json.loads(flask.request.data.decode('utf-8'))
        attributes = envelope['message']['attributes']

        # humps.decamelize turns camelCase to snake_case in dict keys
        return pubsub_dispatch[attributes["action"]](humps.decamelize(attributes))
