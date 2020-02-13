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
        'definitions': {"import_request": NEW_IMPORT_SCHEMA}  # TODO: come up with a way of finding all these
    }
    swagger.init_app(app)
    return app
