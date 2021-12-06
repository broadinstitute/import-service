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
    uuid_and_project = user_auth.workspace_uuid_and_project_with_auth(ws_ns, ws_name, access_token, "write")
    workspace_uuid = uuid_and_project.workspace_id
    google_project = uuid_and_project.google_project

    import_url = request_json["path"]
    import_filetype = request_json["filetype"]
    import_is_upsert = request_json.get("isUpsert", "true") # default to true if missing, to support legacy imports

    # and validate the input's path
    translate.validate_import_url(import_url, import_filetype, user_info)

    # parse is_upsert from a str into a bool
    is_upsert = str(import_is_upsert).strip().lower() == "true"

    new_import = model.Import(
        workspace_name=ws_name,
        workspace_ns=ws_ns,
        workspace_uuid=workspace_uuid,
        workspace_google_project=google_project,
        submitter=user_info.user_email,
        import_url=import_url,
        filetype=request_json["filetype"],
        is_upsert=is_upsert)

    with db.session_ctx() as sess:
        sess.add(new_import)
        new_import_id = new_import.id

    pubsub.publish_self({"action": "translate", "import_id": new_import_id})

    return new_import.to_status_response()
