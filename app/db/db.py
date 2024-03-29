import os
import logging
from contextlib import contextmanager
from typing import Iterator

import sqlalchemy.engine.url
import sqlalchemy.orm

DBSession = sqlalchemy.orm.session.Session

db_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

# Store the db so it can be reused between GAE invocations.
_db = None
_sessionmaker = None


def get_session() -> DBSession:
    global _db
    global _sessionmaker

    if _db is None:
        _db = sqlalchemy.create_engine(
            db_connection_name,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True  # before doing things, send a "ping" request to the db to make sure the connection didn't drop
        )

        from app.db import model
        logging.info("Creating new database tables...")
        model.Base.metadata.create_all(_db)

    if _sessionmaker is None:
        # NOTE on the use of expire_on_commit = False here.
        # We often need to access attributes on an import object after a session is closed.
        # By default, the session will "expire" the object on commit, which makes further attribute access throw an exception.
        # See https://docs.sqlalchemy.org/en/13/orm/session_state_management.html for sqlalchemy object states.
        # expire_on_commit = False disables this behaviour. We pair this with session.expunge_all() when closing a
        # transaction, which detaches the object without invalidating access to its attributes.
        _sessionmaker = sqlalchemy.orm.sessionmaker(bind=_db, expire_on_commit=False)

    return _sessionmaker()


@contextmanager
def session_ctx() -> Iterator[DBSession]:
    """Provide a transactional scope around a series of operations."""
    session = get_session()
    try:
        yield session
        session.commit()
        session.expunge_all()  # see above NOTE in get_session.
    except:
        session.rollback()
        raise
    finally:
        session.close()
