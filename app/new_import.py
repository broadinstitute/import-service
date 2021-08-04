import flask

from app import translate
from app.db import db, model
from app.external import sam, pubsub
from app.auth import user_auth


def handle(request: flask.Request, ws_ns: str, ws_name: str) -> model.ImportStatusResponse:
    access_token = user_auth.extract_auth_token(request)
    user_info = sam.validate_user(access_token)

    # force parsing as json regardless of application/content-type, return None if errors
    request_json = request.get_json(force=True, silent=True)

    # make sure the user is allowed to import to this workspace
    workspace_uuid = user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "write")

    # TODO: AS-155: change to "url"?
    import_url = request_json["path"]
    import_filetype = request_json["filetype"]

    # and validate the input's path
    translate.validate_import_url(import_url, import_filetype, user_info)

    new_import = model.Import(
        workspace_name=ws_name,
        workspace_ns=ws_ns,
        workspace_uuid=workspace_uuid,
        submitter=user_info.user_email,
        import_url=import_url,
        filetype=request_json["filetype"])

    with db.session_ctx() as sess:
        sess.add(new_import)
        new_import_id = new_import.id

    pubsub.publish_self({"action": "translate", "import_id": new_import_id})

    return new_import.to_status_response()
