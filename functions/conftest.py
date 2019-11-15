import flask
import flask.testing
import sqlalchemy.engine
import sqlalchemy.orm
import pytest
from functions import main
from functions.common import db


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


_db = None
_connection = None
Session = sqlalchemy.orm.sessionmaker()


# internal fixture for creating one sqlite db for test and a connection to it
@pytest.fixture(scope="session")
def _dbconn_internal() -> sqlalchemy.engine.Connection:
    global _db, _connection
    _db = sqlalchemy.create_engine('sqlite://')
    from common import model
    model.Base.metadata.create_all(_db)
    _connection = _db.connect()
    yield _connection
    _connection.close()


# fixture for database sessions in tests.
# vanishes everything the test does with a txn rollback
@pytest.fixture(scope="function")
def dbsession(_dbconn_internal) -> db.DBSession:
    txn = _dbconn_internal.begin()
    sess = Session(bind=_dbconn_internal)
    yield sess
    sess.close()
    txn.rollback()
