"""
This module contains fixtures for pytest -- objects that are set up by the test framework and summonable by name.
They can be created and torn down at different scopes: the entire test session, within a module, or around every
test function.
For more info, see here: https://docs.pytest.org/en/latest/fixture.html
"""

from typing import Iterator

import flask
import flask.testing
import pytest
import sqlalchemy.engine
import sqlalchemy.orm

import main
from functions.common import db, model


@pytest.fixture(scope="session")
def client() -> flask.testing.FlaskClient:
    """Builds a Flask client wired up for unit tests. Created once per test invocation and reused thereafter."""
    app = flask.Flask(__name__)
    app.debug = True
    with app.app_context():
        flask.current_app.is_test_fixture = True

    # Cloud Functions forward all HTTP methods.
    # https://cloud.google.com/functions/docs/writing/http#handling_http_methods
    HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

    for f in main.ALL_HTTP_FUNCTIONS:
        # <path:rest> is Flask for "match anything here, including if it has slashes".
        # the bound value would get assigned to the "rest" key in kwargs, but we don't have access
        # to this in GCF-land so we just throw it away and re-implement path matching ourselves.
        app.add_url_rule(f"/{f.__name__}/<path:rest>", f.__name__, lambda **kwargs: f(flask.request), methods=HTTP_METHODS)

    return app.test_client()


_db = None
_connection = None
Session = sqlalchemy.orm.sessionmaker()


@pytest.fixture(scope="session")
def _dbconn_internal() -> Iterator[sqlalchemy.engine.Connection]:
    """Internal fixture for creating one sqlite db for test and a connection to it."""
    global _db, _connection
    _db = sqlalchemy.create_engine('sqlite://')

    model.Base.metadata.create_all(_db)

    _connection = _db.connect()
    yield _connection
    _connection.close()


@pytest.fixture(scope="function", autouse=True)
def dbsession(_dbconn_internal: sqlalchemy.engine.Connection) -> Iterator[db.DBSession]:
    """
    At the start of every test function, create a new session and put it in a transaction.
    This lets us roll back at the end of the test.
    Putting a reference to this function on the Flask app might work, but that would only be
    usable in HTTP cloud functions. Background functions would still need to find another way.
    So until we come up with a better idea, we monkey patch the give me a database session"
    function called by application code. YIKES!!!!"""
    txn = _dbconn_internal.begin()
    sess = Session(bind=_dbconn_internal)
    db.get_session = lambda: sess
    yield sess
    sess.close()
    txn.rollback()
