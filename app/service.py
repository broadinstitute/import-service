import flask
import jsonschema
import logging

from app import translate
from app.util import exceptions
from app.http import httputils
from app.db import db, model
from app.external import sam, pubsub
from app.auth import user_auth

NEW_IMPORT_SCHEMA = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "path": {
      "type": "string"
    },
    "filetype": {
      "type": "string",
      "enum": translate.VALID_FILETYPES
    }
  },
  "required": ["path", "filetype"]
}


schema_validator = jsonschema.Draft7Validator(NEW_IMPORT_SCHEMA)


def handle(request: flask.Request) -> flask.Response:
    request_path = request.path

    urlparams = httputils.expect_urlshape('/iservice/<ws_ns>/<ws_name>/imports', request_path)

    access_token = user_auth.extract_auth_token(request)
    user_info = sam.validate_user(access_token)

    # force parsing as json regardless of application/content-type, return None if errors
    request_json = request.get_json(force=True, silent=True)

    # make sure the user is allowed to import to this workspace
    workspace_uuid = user_auth.workspace_uuid_with_auth(urlparams["ws_ns"], urlparams["ws_name"], access_token, "write")

    try:  # now validate that the input is correctly shaped
        schema_validator.validate(request_json)
    except jsonschema.ValidationError as ve:
        logging.info("Got malformed JSON.")
        raise exceptions.BadJsonException(ve.message)

    import_url = request_json["path"]

    # and validate the input's path
    translate.validate_import_url(import_url, user_info)

    new_import = model.Import(
        workspace_name=urlparams["ws_name"],
        workspace_ns=urlparams["ws_ns"],
        workspace_uuid=workspace_uuid,
        submitter=user_info.user_email,
        import_url=import_url)

    with db.session_ctx() as sess:
        sess.add(new_import)
        sess.commit()
        new_import_id = new_import.id

    pubsub.publish({"action": "translate", "import_id": new_import_id})

    return flask.make_response((str(new_import_id), 200))
