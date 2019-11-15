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


def handle(request: flask.Request) -> flask.Response:
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
