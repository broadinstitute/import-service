import flask
import json
import logging
from sqlalchemy.orm.exc import NoResultFound
from typing import Dict, List

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam
from app.util import exceptions


def handle_get_import_status(request: flask.Request, ws_ns: str, ws_name: str, import_id: str) -> model.ImportStatusResponse:
    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view the workspace containing the import
    user_auth.workspace_uuid_and_project_with_auth(ws_ns, ws_name, access_token, "read")

    try:
        with db.session_ctx() as sess:
            imprt = sess.query(model.Import).\
                filter(model.Import.workspace_namespace == ws_ns).\
                filter(model.Import.workspace_name == ws_name).\
                filter(model.Import.id == import_id).one()
            return imprt.to_status_response()
    except NoResultFound:
        raise exceptions.NotFoundException(message=f"Import {import_id} not found")


def handle_list_import_status(request: flask.Request, ws_ns: str, ws_name: str) -> List[model.ImportStatusResponse]:
    # in the case where someone specifies ?running_only rather than ?running_only=true, assume true
    # this also means that ?running_only=boom is interpreted as true, but that seems okay to me
    running_only = request.args.get("running_only", "False").lower() != "false"

    access_token = user_auth.extract_auth_token(request)
    sam.validate_user(access_token)

    # make sure the user is allowed to view this workspace
    user_auth.workspace_uuid_and_project_with_auth(ws_ns, ws_name, access_token, "read")

    with db.session_ctx() as sess:
        q = sess.query(model.Import).\
            filter(model.Import.workspace_namespace == ws_ns).\
            filter(model.Import.workspace_name == ws_name)
        q = q.filter(model.Import.status.in_(ImportStatus.running_statuses())) if running_only else q
        import_list = q.order_by(model.Import.submit_time.desc()).all()
        import_statuses = [imprt.to_status_response() for imprt in import_list]

        return import_statuses


def external_update_status(msg: Dict[str, str]) -> model.ImportStatusResponse:
    """A trusted external service has told us to update the status for this import.
    Change the status, but sanely.
    It's possible that pub/sub might deliver this message more than once, so we need to account for that too."""
    import_id = msg["import_id"]
    new_status: ImportStatus = ImportStatus.from_string(msg["new_status"])

    # We do not use current_status from the pubsub message in the logic below, but it is very useful for debugging
    # so we ask the sender to include it
    if new_status != ImportStatus.Error and "current_status" not in msg:
        raise exceptions.BadJsonException(f"Missing current_status key from update status request for import {import_id}", audit_log = True)

    update_successful = True
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(import_id, sess)

        # if the import job is already in a terminal state, this is an error.
        if imp.status in ImportStatus.terminal_statuses():
            raise exceptions.TerminalStatusChangeException(import_id, new_status, imp.status)

        # if the import job is already in the requested state, noop. Possibly pub/sub double delivery.
        elif new_status.value == imp.status.value:
            logging.info(f"Attempt to move import {import_id}: from {imp.status} to {imp.status}. Likely pub/sub double delivery.")

        # if the requested state would move the import job backwards, this is an error.
        elif new_status.value < imp.status.value:
            logging.info(f"Attempt to move import {import_id}: from {imp.status} to {imp.status}. Possible pub/sub out of order.")
            raise exceptions.IllegalStatusChangeException(import_id, new_status, imp.status)

        # if the requested state would move the import job forward, attempt to save.
        else: # new_status.value > imp.status.value:
            if new_status == ImportStatus.Error:
                imp.write_error(msg.get("error_message", "External service set this import to Error"))
            else:
                current_status: ImportStatus = ImportStatus.from_string(msg["current_status"])
                update_successful = model.Import.update_status_exclusively(import_id, imp.status, new_status, sess)

    if not update_successful:
        logging.warning(f"Failed to update status for import {import_id}: wanted {current_status}->{new_status}, actually {imp.status}.")

    # This goes back to Pub/Sub, nobody reads it
    return model.ImportStatusResponse(import_id, new_status.name, None)
