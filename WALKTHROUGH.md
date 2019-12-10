# Walkthrough

This is a Python 3.7 project containing two Google Cloud Functions. My focus so far has been on building out the structure of the project to make later development go smoothly. As a result, this walkthrough will touch on a lot of separate pieces -- hopefully in a way that you don't have to think about them too much in future :)

I strongly advise you to keep the GitHub repo (or your IDE) open while reading this, so you can refer to the code in context. Additionally, terminal code in this walkthrough assumes you've set up your Python virtualenvironment as described in the [readme](README.md), so if you haven't done that and see `(venv) $` in some code snippet, you'll need to do that if you want to follow along.

#### What does this do?

Please read the tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

At the time of writing, there are two Google Cloud Functions:

1. The "import service", an HTTP function which accepts some placeholder JSON (that will ultimately be an import request) and saves a new import job to the database;
2. An "import task", a background function triggered from a Pub/Sub message. It looks up a job ID (provided in the Pub/Sub message) in the database.

Neither of these are in their final forms; I wanted to get an example of each working before fleshing them out.

### Finding your way around

A very high-level summary of what you can find in this repo:

`main.py` is the entrypoint file for all cloud functions in this repo. Each function in this file corresponds to a single deployed cloud function.

The implementation of each cloud function can be found in its corresponding module, e.g. the import service is in `functions/service.py`.

Code shared across multiple cloud functions is in `functions/common/`.

Tests live in `functions/tests/`. The special file `functions/conftest.py` is pytest's configuration and fixture-definition file.

At the root of the repo, you can also find:

* The Python requirements file
* Shell scripts for type linting, unit testing, deployment, and smoke testing (`*.sh`)
* Initialization files for mypy (the type linter) and pytest (the test framework)
* `secrets.conf`, a placeholder template-ish file that, when filled in, should be turned into `secrets.yaml`.

# Code walkthrough

## Handlers

#### The Cloud Functions entrypoint `main.py`

Google's implementation of HTTP Cloud Functions is... funny. Under the hood, HTTP functions are a [Flask](https://flask.palletsprojects.com/) app, but the app setup and routing are hidden from you. When deploying the Cloud Function, you specify a function in `main.py` that acts as the entrypoint for an incoming HTTP request. From the point of view of the code, that function is called out of nowhere, with a `flask.Request` object as a parameter. Want Flask to process the request for you in some way, e.g. extracting parameters `foo` and `bar` from the URL `/api/<foo>/<bar>/doSomething`? Tough. You have to it yourself.

In order to keep this project unit-testable, we try to escape the grip of Cloud Functions as soon as possible. Both functions (both HTTP and background) defined in `main.py` thus call out to a handler in a different module as soon as possible. We then test the handlers and rely on integration testing to make sure the Cloud Function hookup works correctly.

There are two functions defined in `main.py`:

`iservice` is the import service, an HTTP Cloud Function. The decorator `@httpify_excs` catches any exceptions the code throws and turns them into their appropriate non-200 HTTP return values. The deployment script (`deployall.sh`) has it triggered by an HTTP call to `/iservice`.

`taskchunk` is a sample "import task", which at this point is just a sample [Background Cloud Function triggered by Pub/Sub](https://cloud.google.com/functions/docs/writing/background#cloud-pubsub-example). It expects all its inputs to come from the Pub/Sub message attributes (a key-value map), and passes that on to the handler.

#### The import service `functions/service.py`

This is the handler for the import service. HTTP Cloud Functions accept all HTTP method types, and pass all calls to any URL under the registered path to your single deployed handler. So if you've deployed your cloud function at `/iservice`, it will receive HTTP calls at `GET /iservice` but also `POST /iservice/beep/1/boop/blorp` and everything in between.

This handler does the following:

* checks to see that the incoming request is a `POST`
* validates the incoming JSON body against a schema using the `jsonschema` library
* creates a new UUID to represent the import request
* saves a new row in the database to represent that import, with the UUID
* returns the UUID in the HTTP response

#### A background function `functions/chunk_task.py`

This is a placeholder background function. When it grows up it wants to be the "chunk" import task, but for now it pulls a parameter out of the Pub/Sub message and looks it up in the database. Because background functions don't return anything, it just logs the result, which you can see in the Cloud Function logs in the GCP console.

## Utilities

There's more to this codebase than three files! A lot of it is either working behind the scenes, or just not hooked up yet. Come with me as we discover the hairy internals...

### Database

This project uses [SQLAlchemy](https://docs.sqlalchemy.org/en/13/) as its database library. SQLAlchemy was chosen because it's the example Google uses when [showing you how to connect to Cloud SQL from Cloud Functions](https://cloud.google.com/sql/docs/mysql/connect-functions#connecting_to); no further research was done. It seems to be popular in the Python community.

SQLAlchemy is an [ORM](https://stackoverflow.com/questions/1279613/what-is-an-orm-how-does-it-work-and-how-should-i-use-one). You probably want to read their [tutorial](https://docs.sqlalchemy.org/en/13/orm/tutorial.html) at some point, though I would stop after you've finished reading the "Querying" section.

**Modelling data** `functions/common/model.py`

Like Scala's Slick library, SQLAlchemy lets you map database rows into classes defined in your code. You can see the mapping for the `Import` class (and corresponding table) in `functions/common/model.py`. The class members define table columns and their types; the `__init__()` constructor creates an instance of the object which you can later add to the database. There's an example of that in the import service.

**Connection and transaction management** `functions/common/db.py`

Google recommends [keeping expensive things in global variables so they can be reused across repeated Cloud Function invocations](https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations). In practice, this means we store both the database connection information and a session object globally. However, reusing the session object across multiple Cloud Function invocations can result in some _very_ strange behaviour if you're not careful to commit your transaction and close the session before the Cloud Function returns. Application code should avoid calling `db.get_session()` directly and instead use `with db.session_ctx() as session`, which will handle cleanup for you.

Note also that tucked away in the definition of `get_session()` is a call to `model.Base.metadata.create_all()`. This is what creates the database tables if they don't exist. To my knowledge it does _not_ handle migrations if the table exists but columns are modified (think Liquibase); at some point in the future it's worth looking into the [Alembic](https://pypi.org/project/alembic/) library for that.

**Warning: SQLAlchemy can be weird**

SQLAlchemy's behaviour can be confusing to people coming from Scala's Slick. In particular, it adds objects to the database lazily, not when you ask it to. This means that if you tell it to add a `Foo` object, and then call `session.execute("SQL select * from foos")`, you might not get back what you expect. See the [Gotchas](GOTCHAS.md#sqlalchemy) for the correct way to do this, as well as some other gotchas you should definitely know.


### Auth\[n/z\] `functions/common/auth.py`

This is still very WIP and hasn't been hooked up to anything yet. The intention here is to allow HTTP Cloud Functions to extract the OAuth bearer token from the header of incoming requests and use it to ask Rawls and Sam if the user has access to the workspace.

### Exception handling `functions/common/httputils.py`, `functions/common/exceptions.py`

Failures raise exceptions which are caught by the `@httpify_excs` decorator used in `main.py`. This decorator is defined at `functions/common/httputils.py`: you can see it takes a function, calls it, catches any exceptions, and returns an HTTP response with that exception's status code. The exception classes are defined in `functions/common/exceptions.py`.

## Testing

This project uses [pytest](https://docs.pytest.org/en/latest/) as its testing framework. It has some nifty features like [test discovery](https://docs.pytest.org/en/latest/goodpractices.html#test-discovery) and summon-able [fixture objects](https://docs.pytest.org/en/latest/fixture.html), the latter of which make testing this project possible.

To run tests:

```
(venv) $ ./pytest.sh
```

### The pytest setup file `conftest.py`

`conftest.py` defines [fixtures](https://docs.pytest.org/en/latest/fixture.html) that can be summoned and used in your tests. (Seriously, read that doc.) The thing to know about pytest fixtures is that if you define a function `foo()` decorated with `@pytest.fixture()`, your test function can take a _variable_ called `foo` as a parameter, and pytest will populate it with the value of the fixture. By the "value" of the fixture, I mean whatever is returned by the `yield` statement. Anything after the `yield` statement runs at the end of the scope defined by the `scope` parameter in `@pytest.fixture`.

In other words:

```python
@pytest.fixture(scope="function")
def zonk() -> str:
  yield "zonk"
  print("zonk fixture cleanup")
  
def test_zonk_bonk(zonk):
  assert zonk == "bonk"
  
def test_zonk_cronk(zonk):
  assert zonk == "cronk"

```

Will do something like this at test runtime:

```
test_zonk_bonk failed: AssertionError "zonk" != "bonk"
zonk fixture cleanup

test_zonk_cronk failed: AssertionError "zonk" != "cronk"
zonk fixture cleanup
```

If you had set the fixture scope to `"session"`, you'd only get the cleanup at the end of the test run.

For testing in our project, we define two important fixtures: a Flask client that wraps all the HTTP cloud function endpoints, and a database session that rolls back after the test completes.

#### The Flask client fixture

If you open `functions/tests/test_service.py` you'll see that some of the test functions take a parameter `client`, and then use it to make requests. This is fixture `client()` in `conftest.py`. This fixture is run once per test run (`scope="session"` in the `@pytest.fixture` decorator), and does the following:

* Creates a Flask app in debug mode
* Registers each HTTP function in `main.py` as an endpoint in the Flask app
* Returns a test client pointing to that app

I mentioned earlier that Google's implementation of HTTP Cloud Functions uses Flask without giving developers proper access to it. In a sane Flask project, we'd have created routes using `@app.route()` [as per normal Flask procedure](https://flask.palletsprojects.com/en/1.1.x/quickstart/#routing), and would simply be able to call `app.test_client()` to get a test client that uses the same routing.

Unfortunately, Google hides the underlying Flask app and its routing from us, so we have to create an entirely new Flask app for testing, hack in the route handlers ourselves, and try to mimic the behaviour of Cloud Functions as much as possible. This silliness is a good reason to move this project to Google App Engine in the future, but that's not an immediate requirement -- the grossness is at least handled in the fixture.

#### The database session fixture

The fixture `dbsession()` in `conftest.py` creates a new database session for every test function invocation. The parameter `autouse=True` in the fixture decorator says to create this fixture at the beginning of _every_ test function, not just where it's requested in the function parameters. This is necessary because it [monkeypatches](https://stackoverflow.com/questions/5626193/what-is-monkey-patching) the `get_session()` function on the `db` module to return a single session whose lifetime is scoped to the test function.

This means that not only tests, but application code run by tests, uses the same session for the duration of the test function. Anything that happens during that session, including any commits and new-session chicanery, happens inside a transaction associated with the database connection. Once the test function completes, the transaction is rolled back and the database is restored to its original (empty) state.

You can see tests covering database behaviour in `functions/tests/test_db.py`.

### Mocking objects

Python testing makes heavy use of mocks, often replacing functions in modules to return a mock object with faked member variables.

You can see some examples in `functions/test/test_sam.py`, where we use mocking to patch the call to `requests.get` to fake an HTTP response without making one for real. If you're feeling brave, `functions/test/test_auth.py` mocks functions on both the Rawls and Sam modules, making some function calls return exceptions and others return predefined values.

Figuring out how to refer to the function you want to patch depends on how your imports are organized. Let's say we want to mock `bar.expensive()` in this examples:

```python
# foo.py
import bar

def somefunc():
    return bar.expensive() + "weee"
```

In order to mock `bar.expensive`, you would patch `foo.bar.expensive`, _not_ `bar.expensive`. The latter would patch the `bar` module, not the version of `bar` that's been imported into `foo.py`.

If instead you imported `bar.expensive` like so:

```python
# foo.py
from bar import expensive

def somefunc():
    return expensive() + "weee"
```

You would need to patch `foo.expensive`, because you've now imported the function call `expensive` directly into the scope of `foo`.

This can take a bit of trial and error to figure out -- and sometimes changing your import statements. The top of `functions/common/auth.py` used to say `from .rawls import *`, but that made it difficult to patch the functions in `rawls`. So I ended up doing `from ..common import rawls`, which made life easier.

For more information on this, see [Where to patch](https://docs.python.org/3/library/unittest.mock.html#where-to-patch) in the documentation for the `mock` library. A better understanding of the difference between `mock.patch()` and `mock.patch.object()` might also help clean the code up; [here](https://stackoverflow.com/questions/29152170/what-is-the-difference-between-mock-patch-object-and-mock-patch) is a starting point if you want to go down that rabbithole.

## Type hinting

This project uses type hinting. You'll see functions defined as `def foo(bar: int) -> str`, which (predictably) means that `bar` should be an `int` and `foo` returns a string.

For a quick overview of type hinting, see [here](https://github.com/broadinstitute/mypy-testing). If you need to debug something in particular, the [mypy documentation](https://mypy.readthedocs.io/en/latest/getting_started.html) is excellent. There's also a [cheat sheet](https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html#cheat-sheet-py3).

You are not required to type hint everything, but you *should* add types for:
* function arguments
* function return types
* class variables

To run the type linter, go to the repo root directory and run:
```
(venv) $ ./mypy.sh
```

You should make mypy happy before opening a PR. Note that errors in some modules will be listed twice. This is annoying, but the good news is that you only have to fix them once.

You may see errors because you're relying on an external library which doesn't have type information. They'll look like this:
```
error: No library stub file for module 'jsonschema'
note: (Stub files are from https://github.com/python/typeshed)
```
If this happens you can add a new section in `mypy.ini` with an `ignore-missing-imports` line to tell it sorry, nothing you can do.

## Deployment and smoke testing

Run `./deployall.sh`. You will need a copy of `secrets.yaml` in the root of the repo, which is not checked in because it's full of secrets. `secrets.conf` is the to-be-templated version, though the secrets aren't stored in vault and there's no templating mechanism built yet. Ask Hussein if you want the secrets.

Secrets are passed in to the Cloud Function as environment variables. Otherwise the deploy script just registers the two functions with their corresponding HTTP and pubsub triggers and turns the timeout up to the max.

In lieu of a proper set of integration tests, there's a very simple smoke test script. Running `./smoketest.sh` will first ping the import service, then put a message on the import task's pubsub queue. It will prompt you to visually check its output.

You'll need to be `gcloud auth login`'d to your `@broadinstitute.org` account for both the deployment and smoke test scripts, as both work in `broad-dsde-dev`.

# Things still to do

This list is separate to the tasks outlined in [the epic](https://broadworkbench.atlassian.net/browse/AS-128), and is very code-oriented.

* Hook up the auth code to the HTTP function so we can authorize users (Hussein hopes to have this done before vaca)
* CI
* Figure out how to stream files to/from GCS buckets without localizing them
* Figure out how to chunk PFBs
* Use [sqlalchemy-repr](https://github.com/manicmaniac/sqlalchemy-repr) to avoid the gross `__repr__` definition in database model classes (low prio)

More involved:
* Move to GAE
* Use [Alembic](https://pypi.org/project/alembic/) for database migrations
