from functions import chunk_task
from functions.common import db, model
from sqlalchemy import inspect


def test_chunk_task(dbsession: db.DBSession):
    fetch = chunk_task.handle({"job_id":"four"}, dbsession)
    assert fetch == []
