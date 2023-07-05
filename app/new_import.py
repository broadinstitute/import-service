import flask
import logging


from app import translate
from app.translate import FILETYPE_TRANSLATORS, FILETYPE_NOTRANSLATION
from app.db import db, model
from app.external import sam, pubsub
from app.auth import user_auth
from app.util import exceptions

from typing import Optional, Set
from urllib.parse import urlparse
import os

from app.auth.userinfo import UserInfo

PROTECTED_NETLOCS = ["anvil.gi.ucsc.edu", "anvilproject.org", "gen3.biodatacatalyst.nhlbi.nih.gov"]

VALID_NETLOCS = PROTECTED_NETLOCS + ["s3.amazonaws.com", "storage.googleapis.com", "service.azul.data.humancellatlas.org", "dev.singlecell.gi.ucsc.edu", "core.windows.net"]


def handle(request: flask.Request, ws_ns: str, ws_name: str) -> model.ImportStatusResponse:
    access_token = user_auth.extract_auth_token(request)
    user_info = sam.validate_user(access_token)

    # force parsing as json regardless of application/content-type, return None if errors
    request_json_opt = request.get_json(force=True, silent=True)

    if not isinstance(request_json_opt, dict):
        raise exceptions.BadJsonException("Input payload is not valid", audit_log = True)

    request_json: dict = request_json_opt

    # make sure the user is allowed to import to this workspace
    uuid_and_project = user_auth.workspace_uuid_and_project_with_auth(ws_ns, ws_name, access_token, "write")
    workspace_uuid = uuid_and_project.workspace_id
    google_project = uuid_and_project.google_project
    authorizationDomain = uuid_and_project.authorizationDomain
    bucketName = uuid_and_project.bucketName



    import_url = request_json["path"]
    import_filetype = request_json["filetype"]
    import_is_upsert = request_json.get("isUpsert", "true") # default to true if missing, to support legacy imports
    options = request_json.get("options",{})
    is_tdr_sync_required = options.get("tdrSyncPermissions", False) # default to not sync permissions

    logging.info(f"New import received for {import_url}, {import_filetype}, is_upsert: {import_is_upsert}, \
        options: {options}, tdrSyncFlag: {is_tdr_sync_required}")

    # and validate the input's path
    actual_netloc = translate.validate_import_url(import_url, import_filetype, user_info)
    # Refuse to import protected data into unprotected workspace
    if is_protected_data(actual_netloc, import_filetype):
        if not is_protected_workspace(authorizationDomain, bucketName):
            raise exceptions.AuthorizationException("Unable to import protected data into an unprotected workspace")

    # parse is_upsert from a str into a bool
    is_upsert = str(import_is_upsert).strip().lower() == "true"

    new_import = model.Import(
        workspace_name=ws_name,
        workspace_ns=ws_ns,
        workspace_uuid=workspace_uuid,
        workspace_google_project=google_project,
        submitter=user_info.user_email,
        import_url=import_url,
        filetype=import_filetype,
        is_upsert=is_upsert,
        is_tdr_sync_required=is_tdr_sync_required)

    with db.session_ctx() as sess:
        sess.add(new_import)
        new_import_id = new_import.id

    pubsub.publish_self({"action": "translate", "import_id": new_import_id})

    return new_import.to_status_response()

def is_protected_workspace(authorizationDomain: Set[str],  bucketName: str):
    if len(authorizationDomain) > 0:
        return True
    return bucketName.startswith("fc-secure")

def validate_import_url(import_url: Optional[str], import_filetype: Optional[str], user_info: UserInfo) -> str:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit.  If no error found with import url,
    return the url host for further validation"""
    # json schema validation ensures that "import_url" exists, but we'll be safe
    if import_url is None:
        logging.info(f"Missing path from inbound translate request:")
        raise exceptions.InvalidPathException(import_url, user_info, "Missing path to file to import")

    # json schema validation ensures that "import_filetype" exists, but we'll be safe
    if import_filetype is None:
        logging.info(f"Missing filetype from inbound translate request:")
        raise exceptions.InvalidFiletypeException(import_filetype, user_info, "Missing filetype")

    try:
        parsedurl = urlparse(import_url)
    except Exception as e:
        # catch any/all exceptions here so we can ensure audit logging
        raise exceptions.InvalidPathException(import_url, user_info, f"{e}")

    # parse path into url parts, verify the netloc is one that we allow
    # we validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches; this allows
    # for subdomains of the netlocs we deem safe.
    # for "rawlsjson" requests, we validate that the file-to-be-imported is already in our
    # dedicated bucket.
    actual_netloc = parsedurl.netloc

    if import_filetype == FILETYPE_NOTRANSLATION and actual_netloc == os.environ.get("BATCH_UPSERT_BUCKET"):
        return actual_netloc
    elif import_filetype in FILETYPE_TRANSLATORS.keys() and any(actual_netloc.endswith(s) for s in VALID_NETLOCS):
        return actual_netloc
    elif import_filetype == "tdrexport" and parsedurl.scheme == "gs":
        return actual_netloc
    elif import_filetype == "tdrexport" and parsedurl.scheme == "https" and any(actual_netloc.endswith(s) for s in VALID_NETLOCS):
        return actual_netloc
    else:
        logging.warning(f"Unrecognized netloc or bucket for import: [{parsedurl.netloc}] from [{import_url}]")
        raise exceptions.InvalidPathException(import_url, user_info, "File cannot be imported from this URL.")

def is_protected_data(import_netloc: str, import_filetype: Optional[str]) -> bool:
    """Determines whether an import is protected data based on where it's imported from
    and its filetype.  Initially, only PFBs from AnVIl are considered protected data"""
    if import_filetype == "pfb":
        return any(import_netloc.endswith(s) for s in PROTECTED_NETLOCS)
    return False
