# import-service
Terra Import Service. Tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

### Repo structure

`functions/main.py` is the entrypoint file for all cloud functions in this repo. Each function in this file corresponds to a single deployed cloud function.

Code shared by cloud functions is in `functions/common`.

### Developer instructions

#### First time setup

Create and activate the Python virtualenvironment:

```
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip install --user -r functions/requirements.txt
```

#### Normal usage

Activate and deactivate the venv.

```
$ source venv/bin/activate
<do all your work here>
(venv) $ deactivate
```

#### Type linting

```
(venv) $ cd functions
(venv) $ mypy *.py
```

Don't check in until these are clean.

#### Testing
```
(venv) $ cd functions
(venv) $ python3 -m pytest
```

If you pass your test function the magic parameter `client` it will be initialized with a Flask client that you can post requests to. For testing purposes, each Cloud Function endpoint is at the name of its function, e.g. posting to `/iservice` will hit the `iservice()` function in `main.py`.
