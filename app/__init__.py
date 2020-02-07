import flask
import flask_compress
from app.server import routes, json_response

compress = flask_compress.Compress()


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.response_class = json_response.JsonResponse
    app.register_blueprint(routes.routes)
    compress.init_app(app)
    return app
