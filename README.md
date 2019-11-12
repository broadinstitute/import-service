# import-service
Terra Import Service. Tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

### Repo structure

`functions/main.py` is the entrypoint file for all cloud functions in this repo. Each function in this file corresponds to a single deployed cloud function.

Code shared by cloud functions is in `functions/common`.

### Getting started

#### First time setup

Create and activate the Python virtualenvironment:

```
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install --user -r functions/requirements.txt
```

#### Normal usage

Activate and deactivate the venv.

```
$ source venv/bin/activate
<do all your work here>
$ deactivate
```
