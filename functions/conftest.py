import flask
import flask.testing
import pytest
from functions import main


@pytest.fixture(scope="module")
def client() -> flask.testing.FlaskClient:
    """Builds a Flask client wired up for unit tests. Created once per test invocation and reused thereafter."""
    app = flask.Flask(__name__)

    # Cloud Functions forward all HTTP methods.
    # https://cloud.google.com/functions/docs/writing/http#handling_http_methods
    HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

    for f in main.ALL_HTTP_FUNCTIONS:
        app.add_url_rule('/'+f.__name__, f.__name__, lambda: f(flask.request), methods=HTTP_METHODS)

    return app.test_client()
