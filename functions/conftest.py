import flask
import flask.testing
import sqlalchemy.engine
import pytest
from functions import main
from functions.common import dbsetup


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


db = None
connection = None


# internal fixture for creating one sqlite db for tests
@pytest.fixture(scope="session")
def _dbconn_internal() -> sqlalchemy.engine.Connection:
    global db, connection
    db = sqlalchemy.create_engine('sqlite://')
    connection = db.connect()
    dbsetup.create_tables(connection)
    yield connection
    connection.close()


# fixture for database connections in tests. vanishes everything the test does with a txn rollback
@pytest.fixture(scope="function")
def dbconn(_dbconn_internal) -> sqlalchemy.engine.Connection:
    txn = _dbconn_internal.begin()
    yield _dbconn_internal
    txn.rollback()
