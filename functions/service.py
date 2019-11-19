import flask
import jsonschema

from .common import db, model

NEW_IMPORT_SCHEMA = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "path":{
      "type": "string"
    },
    "filetype": {
      "type": "string",
      "enum":["pfb"]
    }
  },
  "required": ["path","filetype"]
}


schema_validator = jsonschema.Draft7Validator(NEW_IMPORT_SCHEMA)


def urlchoppy(request: flask.Request):
    import re
    m = re.match(r'/(?P<wsn>\w+)/(?P<ws>\w+)/import', request.path)
    if m is None:
        pass # this is not the request we're looking for
    else:
        m.groupdict() #returns {"wsn": "foo", "ws": "bar" } for request.path="/foo/bar/import"


def handle(request: flask.Request, path_prefix = "") -> flask.Response:
    # in testing this is /iservice/burp/borp/import.
    # TODO: what is it in GCP?
    # I think it's just /burp/borp/import but we can use path_prefix to fix it.
    print(request.path)
    if request.method == 'POST':
        # force parsing as json regardless of application/content-type, return None if errors
        request_json = request.get_json(force=True, silent=True)

        try:  # now validate that the input is correctly shaped
            schema_validator.validate(request_json)
        except jsonschema.ValidationError as ve:
            return flask.make_response((ve.message, 400))

        new_import = model.Import(workspace_name="myws", workspace_ns="myns", submitter="hussein@cool.com")

        with db.session_ctx() as sess:
            sess.add(new_import)
            sess.commit()
            return flask.make_response((str(new_import.id), 200))
    else:
        return flask.make_response((f"Unhandled HTTP method {request.method}", 500))
