import flask
from flask_restx import Api, Resource, fields
import json
import humps
from typing import Dict, Callable, Any

from app import new_import, translate, status, health
from app.db import model
import app.auth.service_auth
from app.server.requestutils import httpify_excs, pubsubify_excs

routes = flask.Blueprint('import-service', __name__)

authorizations = {
    'Bearer': {
        "type": "apiKey",
        "name": "Authorization",
        "in": "header",
        "description": "Use your GCP auth token, i.e. `gcloud auth print-access-token`. Required scopes are [openid, email, profile]. Write `Bearer <yourtoken>` in the box."
    }
}

api = Api(routes, version='1.0', title='Import Service',
          description='import service',
          authorizations=authorizations,
          security=[{"Bearer": "[]"}])

ns = api.namespace('/', description='import handling')


new_import_model = ns.model("NewImport",
                             {"path": fields.String(required=True),
                              "filetype": fields.String(enum=list(translate.FILETYPE_TRANSLATORS.keys()), required=True)})
import_status_response_model = ns.model("ImportStatusResponse", model.ImportStatusResponse.get_model())
health_response_model = ns.model("HealthResponse", health.HealthResponse.get_model(api))


@ns.route('/<workspace_project>/<workspace_name>/imports/<import_id>')
@ns.param('workspace_project', 'Workspace project')
@ns.param('workspace_name', 'Workspace name')
@ns.param('import_id', 'Import id')
class SpecificImport(Resource):
    @httpify_excs
    @ns.marshal_with(import_status_response_model)
    def get(self, workspace_project, workspace_name, import_id):
        """Return status for this import."""
        return status.handle_get_import_status(flask.request, workspace_project, workspace_name, import_id)


@ns.route('/<workspace_project>/<workspace_name>/imports')
@ns.param('workspace_project', 'Workspace project')
@ns.param('workspace_name', 'Workspace name')
class Imports(Resource):
    @httpify_excs
    @ns.expect(new_import_model, validate=True)
    @ns.marshal_with(import_status_response_model, code=201)
    def post(self, workspace_project, workspace_name):
        """Accept an import request."""
        return new_import.handle(flask.request, workspace_project, workspace_name), 201

    @httpify_excs
    @ns.marshal_with(import_status_response_model, code=200, as_list=True)
    def get(self, workspace_project, workspace_name):
        """Return all imports in the workspace."""
        return status.handle_list_import_status(flask.request, workspace_project, workspace_name)


@ns.route('/health')
class Health(Resource):
    @httpify_excs
    @api.doc(security=None)
    @ns.marshal_with(health_response_model, code=200)
    def get(self):
        """Return whether we and all dependent subsystems are healthy."""
        return health.handle_health_check(), 200


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
    @ns.marshal_with(import_status_response_model, code=200)
    def post(self) -> flask.Response:
        app.auth.service_auth.verify_pubsub_jwt(flask.request)

        envelope = json.loads(flask.request.data.decode('utf-8'))
        attributes = envelope['message']['attributes']

        # humps.decamelize turns camelCase to snake_case in dict keys
        return pubsub_dispatch[attributes["action"]](humps.decamelize(attributes))
