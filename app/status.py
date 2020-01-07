import flask
from sqlalchemy import and_

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam


def handle_one(request: flask.Request, ws_ns: str, ws_name: str, import_id: str) -> flask.Response:
    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to import to this workspace
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        import_obj = sess.query(model.Import).filter(model.Import.id ==import_id).one()
        import_status = import_obj.status.name

    return flask.make_response((str(import_status), 200))


def import_list_entry(imprt: model.Import):
    # see https://github.com/dropbox/sqlalchemy-stubs/issues/114 for why type is ignored
    return {"id": imprt.id, "status": imprt.status.name} #type: ignore


def handle_list(request: flask.Request, ws_ns: str, ws_name: str) -> flask.Response:
    running_only = "running_only" in request.args

    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to import to this workspace
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    if running_only:
        with db.session_ctx() as sess:
            import_obj = sess.query(model.Import).filter(and_(model.Import.workspace_namespace == ws_ns,
                                                              model.Import.workspace_name == ws_name,
                                                              model.Import.status == ImportStatus.Running)).all()
            import_statuses = list(map(import_list_entry, import_obj))
            return flask.make_response((str(import_statuses), 200))

    else:
        with db.session_ctx() as sess:
            import_obj = sess.query(model.Import).filter(and_(model.Import.workspace_namespace == ws_ns,
                                                              model.Import.workspace_name == ws_name)).all()
            import_statuses = list(map(import_list_entry, import_obj))
            return flask.make_response((str(import_statuses), 200))
