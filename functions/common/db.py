from contextlib import contextmanager
from typing import Iterator
import os

import sqlalchemy.engine.url
import sqlalchemy.orm

DBSession = sqlalchemy.orm.session.Session

db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_name = os.environ.get("DB_NAME")
cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

# Store the db so it can be reused between Cloud Function invocations.
# Storing the session seems sketchier, and results in strange behaviour if application code isn't
# careful to close its sessions. Application code should use session_ctx() to make sure this is handled
# cleanly.
_db = None
_session = None


def get_session() -> DBSession:
    global _db, _session

    if _db is None:
        _db = sqlalchemy.create_engine(
            # Equivalent URL:
            # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
            sqlalchemy.engine.url.URL(
                drivername='mysql+pymysql',
                username=db_user,
                password=db_pass,
                database=db_name,
                query={
                    'unix_socket': '/cloudsql/{}'.format(cloud_sql_connection_name)
                }
            ),
            pool_size=1,
            max_overflow=0
        )

        from . import model
        model.Base.metadata.create_all(_db)

    if _session is None:
        sessionmaker = sqlalchemy.orm.sessionmaker(bind=_db)
        _session = sessionmaker()

    return _session


@contextmanager
def session_ctx() -> Iterator[DBSession]:
    """Provide a transactional scope around a series of operations."""
    session = get_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
