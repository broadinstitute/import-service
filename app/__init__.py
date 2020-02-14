import flask
from app.server import routes, json_response
from app.server.swagger import swagger
from app.new_import import NEW_IMPORT_SCHEMA

def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    #app.response_class = json_response.JsonResponse
    app.register_blueprint(routes.routes)
    app.config["SWAGGER"] = {
        'openapi': '3.0.2',
        'definitions': { # TODO: a saner way of consolidating all these
            "import_request": NEW_IMPORT_SCHEMA,
            "import_status" : {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "status": { "type": "string" }
                },
                "required": ["id", "status"]
            }
        }
    }
    swagger.init_app(app)
    return app
