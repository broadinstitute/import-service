"""
This module contains fixtures for pytest -- objects that are set up by the test framework and summonable by name.
They can be created and torn down at different scopes: the entire test session, within a module, or around every
test function.
For more info, see here: https://docs.pytest.org/en/latest/fixture.html
"""

from typing import Iterator

import flask.testing
import pytest
import sqlalchemy.engine
import sqlalchemy.orm

from app import create_app
from app.db import db, model, DBSession


def _test_client() -> flask.testing.FlaskClient:
    """Builds a Flask client wired up for unit tests. Created once per test invocation and reused thereafter."""
    app = create_app()
    app.debug = True

    return app.test_client()


@pytest.fixture(scope="session")
def client() -> flask.testing.FlaskClient:
    """A FlaskClient fixture that's created once per test run. You should use this in most instances."""
    return _test_client()


@pytest.fixture(scope="function")
def client_with_modifiable_routes() -> flask.testing.FlaskClient:
    """A FlaskClient fixture that's recreated for every test function. You'll need this if you want to dynamically add
    URLs in your test, otherwise previously running tests will have flipped a switch in the app that asserts because
    you've already handled a request beforehand."""
    return _test_client()


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
def dbsession(_dbconn_internal: sqlalchemy.engine.Connection) -> Iterator[DBSession]:
    """
    At the start of every test function, create a new session and put it in a transaction.
    This lets us roll back at the end of the test.
    Putting a reference to this function on the Flask app might work, but that would only be
    usable in HTTP cloud functions. Background functions would still need to find another way.
    So until we come up with a better idea, we monkey patch the give me a database session"
    function called by application code. YIKES!!!!"""
    txn = _dbconn_internal.begin()
    sess = Session(bind=_dbconn_internal, expire_on_commit=False)
    db.get_session = lambda: sess
    yield sess
    sess.close()
    txn.rollback()


@pytest.fixture(scope="function")
def pubsub_fake_env(monkeypatch) -> Iterator[None]:
    monkeypatch.setenv("PUBSUB_PROJECT", "pubsub-project")
    monkeypatch.setenv("PUBSUB_TOPIC", "pubsub-topic")
    monkeypatch.setenv("PUBSUB_TOPIC", "pubsub-subscription")
    monkeypatch.setenv("PUBSUB_TOKEN", "token")
    monkeypatch.setenv("PUBSUB_AUDIENCE", "aud")
    monkeypatch.setenv("PUBSUB_ACCOUNT", "sa@sa.org")
    yield
