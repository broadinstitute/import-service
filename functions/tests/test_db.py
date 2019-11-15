import datetime
from common import db, model


def test_db_add(dbsession: db.DBSession):
    new_import = model.Import(workspace_name="wsName",
                              workspace_ns="wsNs",
                              submitter="hussein@coolguy.email")
    dbsession.add(new_import)

    # session doesn't write to the db until it's flushed
    unflushed = dbsession.execute("select * from imports").fetchall()
    assert len(unflushed) == 0

    # it's there if we query
    res = dbsession.query(model.Import).all()
    assert len(res) == 1

    # query invokes a flush
    flushed = dbsession.execute("select * from imports").fetchall()
    assert len(flushed) == 1

    # actually commit the txn so other sessions can see it.
    dbsession.commit()


def test_db_rollback(dbsession: db.DBSession):
    # previous test should have been rolled back
    rows = dbsession.execute("select * from imports").fetchall()
    assert len(rows) == 0
