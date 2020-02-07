import flask
import flask_compress
from app.server import routes


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.register_blueprint(routes.routes)
    flask_compress.Compress(app)
    return app
