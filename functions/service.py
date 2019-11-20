import flask
import jsonschema

from functions.common import auth, sam, db, model, exceptions, httputils

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


def handle(request: flask.Request, url_prefix: str) -> flask.Response:
    request_path = httputils.correct_gcf_path(request.path, url_prefix)

    if request.method == 'POST':
        urlparams = httputils.expect_urlshape('/iservice/<ws_ns>/<ws_name>/import', request_path)

        access_token = auth.extract_auth_token(request)
        user_info = sam.validate_user(access_token)

        # force parsing as json regardless of application/content-type, return None if errors
        request_json = request.get_json(force=True, silent=True)

        # make sure the user is allowed to import to this workspace
        workspace_uuid = auth.workspace_uuid_with_auth(urlparams["ws_ns"], urlparams["ws_name"], access_token, "write")

        try:  # now validate that the input is correctly shaped
            schema_validator.validate(request_json)
        except jsonschema.ValidationError as ve:
            raise exceptions.BadJsonException(ve.message)

        new_import = model.Import(
            workspace_name=urlparams["ws_name"],
            workspace_ns=urlparams["ws_ns"],
            submitter=user_info.user_email)
            #TODO: add workspace_uuid here

        with db.session_ctx() as sess:
            sess.add(new_import)
            sess.commit()
            return flask.make_response((str(new_import.id), 200))
    else:
        raise exceptions.MethodNotAllowedException(request.method)
