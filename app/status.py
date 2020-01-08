import flask
import json
import sqlalchemy
from sqlalchemy import and_
from sqlalchemy.orm.exc import NoResultFound

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam


def handle_get_import_status(request: flask.Request, ws_ns: str, ws_name: str, import_id: str) -> flask.Response:
    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view the workspace containing the import
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    try:
        with db.session_ctx() as sess:
            imprt = sess.query(model.Import).filter(model.Import.id == import_id).one()
            return flask.make_response((json.dumps({"id": imprt.id, "status": imprt.status.name}), 200))
    except NoResultFound:
        return flask.make_response(f"Import {import_id} either does not exist or you do not have access to view it", 404)


def handle_list_import_status(request: flask.Request, ws_ns: str, ws_name: str) -> flask.Response:
    running_only = "running_only" in request.args

    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view this workspace
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        q = sess.query(model.Import).filter(model.Import.workspace_namespace == ws_ns).filter(model.Import.workspace_name == ws_name)
        q = q.filter(model.Import.status == ImportStatus.Running) if running_only else q
        import_list = q.order_by(model.Import.submit_time).all()
        import_statuses = [{"id": imprt.id, "status": imprt.status.name} for imprt in import_list]

        return flask.make_response((json.dumps(import_statuses), 200))
