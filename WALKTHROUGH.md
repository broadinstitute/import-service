# Walkthrough

This is a Python 3.7 project containing a Google App Engine application, which is also a [Flask](https://flask.palletsprojects.com/) application.

I strongly advise you to keep the GitHub repo (or your IDE) open while reading this, so you can refer to the code in context. Additionally, terminal code in this walkthrough assumes you've set up your Python virtualenvironment as described in the [readme](README.md), so if you haven't done that and see `(venv) $` in some code snippet, you'll need to do that if you want to follow along.

#### What does this do?

It accepts an import request through HTTP (a path to a gen3 .pfb file), translates it to the Rawls batchUpsert format, and then passes that file on to Rawls to import into the user's chosen workspace.

The tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#) is out of date but gives some sense of its place within the Terra ecosystem.

### Finding your way around

A very high-level summary of what you can find in this repo:

`main.py` is the entrypoint. All it does is create the Flask app.

Pretty much everything else lives under `/app`. The files directly inside the `/app` directory are the main business logic of the service.

The other important file is `/app/server/routes.py`. This lists the application's endpoints, dispatching out to other classes to actually handle the requests.

Tests live in `app/tests/`. The special file `app/tests/conftest.py` is pytest's configuration and fixture-definition file.


| directory | description |
|-----|------------------------------------------------------------------------------|
| `/app/auth` | Holds authn/z code for both the application's service account, and the user. |
| `/app/db`  | Database and table definitions.   |
| `/app/external`  | Functions that call other GCP and Terra services.  |
| `/app/server`  | Code handling incoming requests to this application. |
| `/app/translators`  | Code to translate between various filetypes (e.g. pfb to Rawls batchUpsert). |
| `/app/util`  | Utilities, like exception classes. |


At the root of the repo, you can also find:

* The Python requirements file
* Shell scripts for type linting, unit testing, deployment, and smoke testing (`*.sh`)
* Initialization files for mypy (the type linter) and pytest (the test framework)
* `app.yaml.ctmpl`, a placeholder template-ish file that, when filled in, should be turned into `app.yaml`.

# Code walkthrough

## Route handling `/app/server/routes.py`

Each function in this file corresponds to one endpoint that the application exposes. It handles URL parsing but mainly dispatches messages out to other modules. The decorator `@httpify_excs` catches any exceptions the code throws and turns them into their appropriate non-200 HTTP return values. If the code is trying to operate on a particular import, the exception has the option to set the import to its `Error` state and populate a message. This code lives in `/app/server/requestutils.py`.

The special route `/_ah/push-handlers/receive_messages` is the endpoint that GCP Pub/Sub hits in the service's [push subscription](https://cloud.google.com/pubsub/docs/push). We expect that all incoming messages store their information only in message attributes, not the message's bytes-data. We switch on the `action` key to determine which handler gets the message. The decorator `@pubsubify_excs` is the pubsub version of `@httpify_excs`; it's slightly different since Google will retry pubsub message delivery based on the returned HTTP status code.

## Handlers

#### Creating new imports `/app/new_import.py`

This is the handler for creating new imports. It does the following:

* extracts the user's auth token and calls Sam as them to see if they're a real user
* calls Rawls with the requested workspace to see if the user has permission to import entities (aka workspace-write)
* validates the incoming JSON body against a schema using the `jsonschema` library
* creates a new UUID to represent the import request
* saves a new row in the database to represent that import, with the UUID
* puts a message on the import service Pub/Sub queue to translate the new file
* returns the UUID in the HTTP response

#### File translation `/app/translate.py`

Handles translation of files from one type to another. Ultimately, this grabs the file from the path the user provided, opens a file-like object to GCS to put the translated batchUpsert file, and then does the translation. Classes to translate between formats live in `/app/translators/`.

It is important that everything here streams, as GAE VMs have a 2GB memory limit:
* Streaming down the file from the source, which under the hood uses [urllib.request.urlopen](https://docs.python.org/3/library/urllib.request.html#urllib.request.urlopen).
* Streaming the translation. The actual translation function `PFBToRawls.translate()` returns an iterator that yields new elements from each record in the read PFB. The underlying `pypfb` library also streams over the file.
* Converting the individual batchUpsert objects to JSON uses the [iterencode](https://docs.python.org/3/library/json.html#json.JSONEncoder.iterencode) function of Python's native json library.
* `gcsfs` library gives a file-like object that can be incrementally written to.

My use of the word "stream" here isn't really Pythonic vocabulary. [Iterators, generators, and generator expressions](https://anandology.com/python-practice-book/iterators.html) can be strung together to do entire transformations element-wise. That link has more information, but if you see `for x in foo`, `yield`, or parens where you might expect to see square brackets in a list comprehension, like `(x for x in foo)` -- then you're in iterator-land, and you can chain operations on them together without keeping the entire collection in memory.

Once the translation is complete it sends a pubsub message to Rawls to import the batchUpsert file.

#### Status handling `/app/status.py`

This handles getting the status for existing imports. It also provides a pubsub message that "trusted external services" (i.e. Rawls, which is the only other SA allowed to write to the import service's pubsub queue) can send to update the status of an import.


## The guts

### Database

This project uses [SQLAlchemy](https://docs.sqlalchemy.org/en/13/) as its database library. SQLAlchemy was chosen because it's the example Google uses when [showing you how to connect to Cloud SQL from Python](https://cloud.google.com/sql/docs/mysql/connect-functions#connecting_to); no further research was done. It seems to be popular in the Python community.

SQLAlchemy is an [ORM](https://stackoverflow.com/questions/1279613/what-is-an-orm-how-does-it-work-and-how-should-i-use-one). You probably want to read their [tutorial](https://docs.sqlalchemy.org/en/13/orm/tutorial.html) at some point, though I would stop after you've finished reading the "Querying" section.

**Modelling data** `/app/db/model.py`

Like Scala's Slick library, SQLAlchemy lets you map database rows into classes defined in your code. You can see the mapping for the `Import` class (and corresponding table) in `/app/db/model.py`. The class members define table columns and their types; the `__init__()` constructor creates an instance of the object which you can later add to the database.

**Connection and transaction management** `/app/db/db.py`

GAE maintains global variables across repeated invocations that use the same VM, so you can skip doing expensive setup operations by leaving thigns there. We do this with spinning up the database engine. To get hold of a database session so you can do queries, you should generally use `with db.session_ctx() as session`, which will automatically commit for you once the `with` block completes, and rollback if there's an exception.

Note also that tucked away in the definition of `get_session()` is a call to `model.Base.metadata.create_all()`. This is what creates the database tables if they don't exist. It does _not_ handle migrations if the table exists but columns are modified (think Liquibase); we have an [open ticket](https://broadworkbench.atlassian.net/browse/AS-184) to incorporate some library to do that, probably [Alembic](https://pypi.org/project/alembic/).

**Warning: SQLAlchemy can be weird**

SQLAlchemy's behaviour can be confusing to people coming from Scala's Slick. In particular, it adds objects to the database lazily, not when you ask it to. This means that if you tell it to add a `Foo` object, and then call `session.execute("SQL select * from foos")`, you might not get back what you expect. See the [Gotchas](GOTCHAS.md#sqlalchemy) for the correct way to do this, as well as some other gotchas you should definitely know.


### Auth\[n/z\] `/app/auth/*.py`

`service_auth.py` allows you to get both OAuth access tokens and `Credentials` objects to use in the Google Cloud client libraries for Python, using the import service's service account. `user_auth.py` covers finding out if the user is allowed to do things, usually by borrowing the access token they sent in their request to identify as them to Rawls.


### Exception handling `/app/server/requestutils.py`, `/app/util/exceptions.py`

Failures raise exceptions which are caught by the `@httpify_excs` decorator used in `routes.py`. This decorator is defined at `/app/server/requestutils.py`: you can see it takes a function, calls it, catches any exceptions, and returns an HTTP response with that exception's status code. Same goes for `@pubsubify_excs` as described above. The exception classes are defined in `/app/util/exceptions.py`.

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

# TODO keep going from here

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
