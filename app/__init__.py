import flask
from app import routes


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.register_blueprint(routes.routes)
    return app
