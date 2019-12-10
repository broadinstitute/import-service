# Gotchas

This document outlines "gotchas" -- some surprises it's good to watch out for.

## SQLAlchemy

This project uses [SQLAlchemy](https://docs.sqlalchemy.org/en/13/) as its database library. It's an ORM and its behaviour can be surprising if you're not used to it. Here are some tips:

#### Avoid using `execute()`

`execute()` always queries the database directly. This can be bad because [sqlalchemy does not immediately flush your commands to the database](https://docs.sqlalchemy.org/en/13/orm/session_basics.html#flushing), so you might think you've added a row and then find it doesn't show up when you `execute("select * from foo")`. Use the higher-level SQLAlchemy functions like `query()` instead.

#### Remember to `commit()`

Remember to call `commit()` to actually commit your session's transaction to the database! Note also that `commit()` [opens a new transaction for you the next time you do something in the session](https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.commit), so it's okay to call `commit()` multiple times in your function execution.

#### Manage your sessions carefully

Running the same Cloud Function twice (or more) in quick succession will reuse the instance, preserving global state. Sessions are global state, so not closing your session at the end of your CF invocation can lead to unexpected behaviour (like not seeing objects in the database).

In application code, you can make sure that your session always get closed by asking for a session with `session_ctx()`:

```python
def get_all_imports() -> List[Import]:
    with db.session_ctx() as session:
        all_imports = session.query(Import).all()
        return all_imports
```

In test code, you may use either `session_ctx()` or `db.get_session()` without worrying about cleaning up. The test harness creates a new transaction at the beginning of each test function, creates a session inside that, and hands out that same session whenever anyone asks for one. At test teardown time, the session is closed and the transaction is rolled back, restoring the database to its empty state.
 