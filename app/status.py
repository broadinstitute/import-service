import flask
from sqlalchemy import and_

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam


def import_list_entry(imprt: model.Import):
    # see https://github.com/dropbox/sqlalchemy-stubs/issues/114 for why type is ignored
    return {"id": imprt.id, "status": imprt.status.name} #type: ignore


def handle_get_import_status(request: flask.Request, ws_ns: str, ws_name: str, import_id: str) -> flask.Response:
    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view the workspace containing the import
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        imprt = sess.query(model.Import).filter(model.Import.id ==import_id).one()

        return flask.make_response((str(import_list_entry(imprt)), 200))


def select_clauses(running_only: bool, ws_ns: str, ws_name: str):
    if running_only:
        return and_(model.Import.workspace_namespace == ws_ns,
                    model.Import.workspace_name == ws_name,
                    model.Import.status == ImportStatus.Running)
    else:
        return and_(model.Import.workspace_namespace == ws_ns,
                    model.Import.workspace_name == ws_name)


def handle_list_import_status(request: flask.Request, ws_ns: str, ws_name: str) -> flask.Response:
    running_only = "running_only" in request.args

    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view this workspace
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        import_obj = sess.query(model.Import).filter(select_clauses(running_only, ws_ns, ws_name)).all()
        import_statuses = list(map(import_list_entry, import_obj))

        return flask.make_response((str(import_statuses), 200))
