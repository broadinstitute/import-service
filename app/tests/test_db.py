from app.db import db, model


def test_db_add():
    new_import = model.Import(workspace_name="wsName",
                              workspace_ns="wsNs",
                              submitter="hussein@coolguy.email")

    dbsession = db.get_session()
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


def test_db_rollback():
    # previous test should have been rolled back
    dbsession = db.get_session()
    rows = dbsession.execute("select * from imports").fetchall()
    assert len(rows) == 0


def test_db_session_ctx_close():
    """Sanity check to ensure that session.close() is idempotent.
    This is important because application code WILL use the session_ctx,
    and app.tests will again close the session at function teardown."""
    with db.session_ctx() as session:
        new_import = model.Import("aa", "aa", "aa@aa.aa")
        session.add(new_import)
        session.commit()
        session.close()
        session.close()

    # also, there will inevitably be a test that does a query to the db
    # after the application has closed the session, so make sure this works too
    # (it does work, because a new session-transaction is immediately created
    # on session close, see https://bit.ly/33S307r )
    all_imports = db.get_session().query(model.Import).all()
    assert len(all_imports) == 1
