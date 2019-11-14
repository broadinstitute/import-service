import sqlalchemy.engine
from functions import chunk_task


def test_database(dbconn: sqlalchemy.engine.Connection):
    fetch = chunk_task.handle({"job_id":"four"}, dbconn)
    assert fetch == []
