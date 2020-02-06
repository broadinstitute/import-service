# import-service
Terra Import Service. Tech doc [here](https://docs.google.com/document/d/1MeL9J5UqhtCg6SLD2Z9S_SsX3L9jYlZnSpfn2HJptc8/edit#).

A walkthrough of the code in this repo is available at [WALKTHROUGH.md](WALKTHROUGH.md).

## Developer notes

### First time setup

Create and activate the Python virtualenvironment:

```
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip install --user -r requirements.txt
```

You should periodically run the `pip install` line within your venv to keep it up-to-date with changes in dependencies.

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

To run the type linter, go to the repo root directory and run:
```
(venv) $ ./mypy.sh
```

You should make mypy happy before opening a PR. Note that errors in some modules will be listed twice. This is annoying, but the good news is that you only have to fix them once.

### Deployment

Run `./deployall.sh`. You will need a copy of `secrets.yaml` in the root of the repo, which is not checked in because it's full of secrets. `secrets.conf` is the to-be-templated version, though the secrets aren't stored in vault and there's no templating mechanism built yet. Ask Hussein if you want the secrets.

To smoke test a deployment, run `./smoketest.sh`. This will do a few things and prompts you to visually check the output to see if it looks right.