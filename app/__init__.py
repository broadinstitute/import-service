import flask
from app.server import routes


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.register_blueprint(routes.routes)
    app.config["RESTX_MASK_SWAGGER"] = False  # disable X-Fields header in swagger
    return app
