import flask
import flask_compress
from app.server import routes, json_response


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.response_class = json_response.JsonResponse
    app.register_blueprint(routes.routes)
    flask_compress.Compress(app)
    return app
