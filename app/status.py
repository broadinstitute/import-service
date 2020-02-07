import flask
import json
import logging
from sqlalchemy.orm.exc import NoResultFound
from typing import Dict

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam
from app.util import exceptions


def handle_get_import_status(request: flask.Request, ws_ns: str, ws_name: str, import_id: str) -> flask.Response:
    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view the workspace containing the import
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    try:
        with db.session_ctx() as sess:
            imprt = sess.query(model.Import).\
                filter(model.Import.workspace_namespace == ws_ns).\
                filter(model.Import.workspace_name == ws_name).\
                filter(model.Import.id == import_id).one()
            return flask.make_response((json.dumps({"id": imprt.id, "status": imprt.status.name}), 200))
    except NoResultFound:
        raise exceptions.NotFoundException(message=f"Import {import_id} not found")


def handle_list_import_status(request: flask.Request, ws_ns: str, ws_name: str) -> flask.Response:
    running_only = "running_only" in request.args

    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view this workspace
    user_auth.workspace_uuid_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        q = sess.query(model.Import).\
            filter(model.Import.workspace_namespace == ws_ns).\
            filter(model.Import.workspace_name == ws_name)
        q = q.filter(model.Import.status.in_(ImportStatus.running_statuses())) if running_only else q
        import_list = q.order_by(model.Import.submit_time.desc()).all()
        import_statuses = [{"id": imprt.id, "status": imprt.status.name} for imprt in import_list]

        return flask.make_response((json.dumps(import_statuses), 200))


def external_update_status(msg: Dict[str, str]) -> flask.Response:
    """A trusted external service has told us to update the status for this import.
    Change the status, but sanely.
    It's possible that pub/sub might deliver this message more than once, so we need to account for that too."""
    import_id = msg["import_id"]
    new_status: ImportStatus = ImportStatus.from_string(msg["new_status"])

    if new_status != ImportStatus.Error and "current_status" not in msg:
        raise exceptions.BadJsonException(f"Missing current_status key from update status request for import {import_id}", audit_log = True)

    update_successful = True
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(import_id, sess)

        # Only think about updating if the statuses are different.
        if new_status != imp.status:
            # Quick summary:
            #   If the caller is setting to error, ignore current status and jump straight there.
            #   If the import is already in a terminal status, the caller did something bad.
            #   Otherwise update the status if the caller got the previous one correct.
            if new_status == ImportStatus.Error:
                imp.write_error(msg.get("error_message", "External service set this import to Error"))

            elif imp.status in ImportStatus.terminal_statuses():
                raise exceptions.TerminalStatusChangeException(import_id, new_status, imp.status)

            else:
                current_status: ImportStatus = ImportStatus.from_string(msg["current_status"])
                update_successful = model.Import.update_status_exclusively(import_id, current_status, new_status, sess)
        else:
            logging.info(f"Attempt to move import {import_id}: from {imp.status} to {imp.status}. Likely pub/sub double delivery.")

    if not update_successful:
        logging.warning(f"Failed to update status for import {import_id}: expected {current_status}, got {imp.status}.")

    return flask.make_response("ok")
