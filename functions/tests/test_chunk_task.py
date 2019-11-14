import sqlalchemy.engine
from functions import chunk_task


def test_database(dbconn: sqlalchemy.engine.Connection):
    fetch = chunk_task.handle({"job_id":"four"}, dbconn)
    assert fetch == []


def test_db_add(dbconn: sqlalchemy.engine.Connection):
    dbconn.execute(f"""INSERT INTO imports
    VALUES ('import1', 'myWorkspace', 'myProject', 'hussein@coolguy.com', 5, 'Pending')""")
    res = dbconn.execute("select * from imports").fetchall()
    assert len(res) == 1
    print(res)


def test_db_rollback(dbconn: sqlalchemy.engine.Connection):
    rows = dbconn.execute("select * from imports").fetchall()
    assert len(rows) == 0
    print("no rows")
