# import-service
Terra Import Service. Tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

## Finding your way around

All Python code lives in `functions/`. Other elements of this service, like deployment scripts, can be found in the root of the repo.

`functions/main.py` is the entrypoint file for all cloud functions in this repo. Each function in this file corresponds to a single deployed cloud function. The implementation of each can be found in its corresponding module, e.g. the import service is in `functions/service.py`.

Code shared across multiple cloud functions is in `functions/common/`.

Tests live in `functions/tests/`. The special file `functions/conftest.py` is pytest's configuration and fixture-definition file.

## Developer notes

### First time setup

Create and activate the Python virtualenvironment:

```
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip install --user -r requirements.txt
```

### Normal usage

Activate and deactivate the venv:
```
$ source venv/bin/activate
<do all your work here>
(venv) $ deactivate
```

To run tests:
```
(venv) $ ./pytest.sh
```

### Type linting

This project uses type linting. To run the type linter, go to the repo root directory and run:
```
(venv) $ ./mypy.sh
```

You should make the linter happy before opening a PR.

You are not required to type hint everything, but you *should* add types for:
* function arguments
* function return types
* class variables

If you are relying on an external library which doesn't have type stubs, you can add a new section in `mypy.ini` to tell it sorry, nothing you can do.

### Writing tests

If you pass your test function the magic parameter `client` it will be initialized with a Flask client that you can post requests to. For testing purposes, each Cloud Function endpoint is at the name of its function, e.g. posting to `/iservice` will hit the `iservice()` function in `main.py`.

### Deployment

Run `./deployall.sh`. You will need a copy of `secrets.yaml` in the root of the repo, which is currently not checked in or templated. Ask Hussein if you want a copy.

To smoke test a deployment, run `./smoketest.sh`. This will do a few things and prompts you to visually check the output to see if it looks right.

### Notes on SQLAlchemy

This project uses SQLAlchemy as its database library. It's an ORM and its behaviour can be surprising if you're not used to it.

##### Avoid using `execute()`

`execute()` always queries the database directly. This can be bad because [sqlalchemy does not immediately flush your commands to the database](https://docs.sqlalchemy.org/en/13/orm/session_basics.html#flushing), so you might think you've added a row and then find it doesn't show up when you `execute("select * from foo")`. Use the higher-level SQLAlchemy functions like `query()` instead.

##### Remember to `commit()`

Remember to call `commit()` to actually commit your session's transaction to the database! Note also that `commit()` [opens a new transaction for you the next time you do something in the session](https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.commit), so it's okay to call `commit()` multiple times in your function execution.

##### Manage your sessions carefully

Sessions can interact with Cloud Functions weirdly. Running the same Cloud Function twice (or more) in quick succession will reuse the instance, preserving global state. Not closing your session at the end of your CF invocation can lead to unexpected behaviour (like not seeing objects in the database).

In application code, you can make sure that your session always get closed by asking for a session with `session_ctx()`:

```python
def foo():
    with db.session_ctx() as session:
        all_imports = session.query(Import).all()
        return all_imports
```

In test code, you may use either `session_ctx()` or `db.get_session()` without worrying about cleaning up. The test harness creates a new transaction at the beginning of each test function, creates a session inside that, and hands out that same session whenever anyone asks for one. At test teardown time, the session is closed and the transaction is rolled back
